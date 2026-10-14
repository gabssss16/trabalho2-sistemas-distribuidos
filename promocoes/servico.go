package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/mail"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/persistencia"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type inscricao struct {
	Ultimo     base.InteressePromocaoPayload `json:"ultimo_evento"`
	Categorias []string                      `json:"categorias_ativas"`
}

type mensagemEmail struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}

type notificacao struct {
	ID        string        `json:"id"`
	Categoria string        `json:"categoria"`
	Email     mensagemEmail `json:"email"`
	ExpiraEm  time.Time     `json:"expira_em"`
}

type estadoPromocoes struct {
	Inscricoes map[string]inscricao   `json:"inscricoes"`
	Pendentes  map[string]notificacao `json:"pendentes"`
}

type servicoPromocoes struct {
	mu                 sync.Mutex
	caminho, remetente string
	estado             estadoPromocoes
	enviar             func(context.Context, notificacao) error
}

func normalizarInteresse(i base.InteressePromocaoPayload) (base.InteressePromocaoPayload, error) {
	i.Email = strings.ToLower(strings.TrimSpace(i.Email))
	a, err := mail.ParseAddress(i.Email)
	if err != nil || a.Address != i.Email || !strings.Contains(i.Email, "@") {
		return i, fmt.Errorf("%w: email inválido", rabbitmq.ErrEventoInvalido)
	}
	if i.Versao <= 0 || (i.Acao != "cadastrar" && i.Acao != "cancelar") {
		return i, fmt.Errorf("%w: acao ou versao inválida", rabbitmq.ErrEventoInvalido)
	}
	i.Categorias = slices.Clone(i.Categorias)
	for n, c := range i.Categorias {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c != "A" && c != "B" && c != "C" {
			return i, fmt.Errorf("%w: categoria deve ser A, B ou C", rabbitmq.ErrEventoInvalido)
		}
		i.Categorias[n] = c
	}
	slices.Sort(i.Categorias)
	i.Categorias = slices.Compact(i.Categorias)
	if len(i.Categorias) == 0 {
		i.Categorias = nil
		if i.Acao == "cadastrar" {
			return i, fmt.Errorf("%w: informe categorias", rabbitmq.ErrEventoInvalido)
		}
	}
	return i, nil
}

func lerInteresse(d amqp.Delivery) (base.InteressePromocaoPayload, error) {
	var i base.InteressePromocaoPayload
	e, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return i, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if e.Type != base.InteressePromocao || d.RoutingKey != base.InteressePromocao || e.Producer != "principal" {
		return i, fmt.Errorf("%w: esperado interesse.promocao do principal", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(e, "promocoes/keys/principal_public.pem")
	if err != nil || !ok {
		return i, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(e, &i); err != nil {
		return i, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	return normalizarInteresse(i)
}

func abrirPromocoes(caminho, remetente string) (*servicoPromocoes, error) {
	s := &servicoPromocoes{caminho: caminho, remetente: remetente, estado: estadoPromocoes{Inscricoes: make(map[string]inscricao), Pendentes: make(map[string]notificacao)}}
	b, err := os.ReadFile(caminho)
	if errors.Is(err, os.ErrNotExist) {
		return s, persistencia.Salvar(caminho, s.estado)
	}
	if err != nil {
		return nil, err
	}
	var estado estadoPromocoes
	if err := json.Unmarshal(b, &estado); err != nil {
		return nil, err
	}
	s.estado = estado
	if s.estado.Inscricoes == nil || s.estado.Pendentes == nil {
		return nil, fmt.Errorf("arquivo de promoções inválido")
	}
	for email, ins := range s.estado.Inscricoes {
		i, err := normalizarInteresse(ins.Ultimo)
		if err != nil || email != i.Email || !reflect.DeepEqual(i, ins.Ultimo) {
			return nil, fmt.Errorf("inscrição inválida no arquivo")
		}
		for _, c := range ins.Categorias {
			if c != "A" && c != "B" && c != "C" {
				return nil, fmt.Errorf("categoria ativa inválida")
			}
		}
	}
	for id, n := range s.estado.Pendentes {
		if id == "" || n.ID != id || len(n.Email.To) != 1 || n.Email.From == "" || n.Email.Subject == "" || n.Email.Text == "" || n.ExpiraEm.IsZero() || !slices.Contains(s.estado.Inscricoes[n.Email.To[0]].Categorias, n.Categoria) {
			return nil, fmt.Errorf("notificação pendente inválida")
		}
	}
	return s, nil
}

// Sempre sob mutex; cópia antes de gravar preserva a memória em caso de erro.
func (s *servicoPromocoes) copiarEstado() estadoPromocoes {
	e := estadoPromocoes{Inscricoes: make(map[string]inscricao), Pendentes: make(map[string]notificacao)}
	for email, i := range s.estado.Inscricoes {
		e.Inscricoes[email] = i
	}
	for id, n := range s.estado.Pendentes {
		e.Pendentes[id] = n
	}
	return e
}

func (s *servicoPromocoes) salvar(e estadoPromocoes) error {
	if err := persistencia.Salvar(s.caminho, e); err != nil {
		return err
	}
	s.estado = e
	return nil
}

func (s *servicoPromocoes) atualizarInteresse(i base.InteressePromocaoPayload) error {
	i, err := normalizarInteresse(i)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	anterior := s.estado.Inscricoes[i.Email]
	if i.Versao < anterior.Ultimo.Versao {
		return nil
	}
	if i.Versao == anterior.Ultimo.Versao {
		if !reflect.DeepEqual(i, anterior.Ultimo) {
			return fmt.Errorf("%w: versão reutilizada com outros dados", rabbitmq.ErrEventoInvalido)
		}
		return nil
	}
	categorias := slices.Clone(i.Categorias)
	if i.Acao == "cancelar" {
		categorias = nil
		if len(i.Categorias) > 0 {
			for _, c := range anterior.Categorias {
				if !slices.Contains(i.Categorias, c) {
					categorias = append(categorias, c)
				}
			}
		}
	}
	e := s.copiarEstado()
	e.Inscricoes[i.Email] = inscricao{Ultimo: i, Categorias: categorias}
	for id, n := range e.Pendentes {
		if n.Email.To[0] == i.Email && !slices.Contains(categorias, n.Categoria) {
			delete(e.Pendentes, id)
		}
	}
	return s.salvar(e)
}

func (s *servicoPromocoes) agendar(p Promocao) error {
	if p.Produto == "" || p.Desconto <= 0 || p.Desconto >= 100 || (p.Categoria != "A" && p.Categoria != "B" && p.Categoria != "C") {
		return fmt.Errorf("promoção inválida")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.copiarEstado()
	for email, i := range e.Inscricoes {
		if !slices.Contains(i.Categorias, p.Categoria) {
			continue
		}
		var aleatorio [16]byte
		if _, err := rand.Read(aleatorio[:]); err != nil {
			return err
		}
		id := hex.EncodeToString(aleatorio[:])
		e.Pendentes[id] = notificacao{
			ID: id, Categoria: p.Categoria, ExpiraEm: time.Now().UTC().Add(23 * time.Hour),
			Email: mensagemEmail{From: s.remetente, To: []string{email}, Subject: fmt.Sprintf("Promoção: %s com %d%% de desconto", p.Produto, p.Desconto), Text: fmt.Sprintf("%s está com %d%% de desconto! Categoria: %s.\n\nVocê recebe esta oferta porque cadastrou interesse nesta categoria. Para cancelar, altere suas preferências na loja.", p.Produto, p.Desconto, p.Categoria)},
		}
	}
	if len(e.Pendentes) == len(s.estado.Pendentes) {
		return nil
	}
	return s.salvar(e)
}

func (s *servicoPromocoes) enviarPendente(ctx context.Context, id string) error {
	// O cancelamento é serializado com o início e a conclusão de cada envio.
	// Uma chamada HTTP já iniciada pode concluir antes de aplicar o cancelamento.
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.estado.Pendentes[id]
	if !ok {
		return nil
	}
	expirada := !time.Now().Before(n.ExpiraEm)
	ativa := slices.Contains(s.estado.Inscricoes[n.Email.To[0]].Categorias, n.Categoria)
	if !expirada && ativa {
		if err := s.enviar(ctx, n); err != nil {
			return err
		}
	}
	e := s.copiarEstado()
	delete(e.Pendentes, id)
	if err := s.salvar(e); err != nil {
		return err
	}
	if expirada {
		log.Printf("[PROMOÇÕES] Notificação %s expirada sem nova tentativa", id)
	}
	return nil
}

func (s *servicoPromocoes) processarPendentes(ctx context.Context) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.estado.Pendentes))
	for id := range s.estado.Pendentes {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if err := s.enviarPendente(ctx, id); err != nil {
			log.Printf("[PROMOÇÕES] Notificação %s pendente: %v", id, err)
		}
	}
}

func (s *servicoPromocoes) repetirPendentes(ctx context.Context) {
	s.processarPendentes(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processarPendentes(ctx)
		}
	}
}
