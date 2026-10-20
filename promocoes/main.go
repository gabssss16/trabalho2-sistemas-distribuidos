package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	mrand "math/rand"
	"net/http"
	"net/mail"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

// envio de email

type clienteEmail struct {
	apiKey, endpoint string
	http             *http.Client
}

func novoClienteEmail(apiKey, remetente string) (*clienteEmail, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("configure RESEND_API_KEY para enviar promoções")
	}
	if _, err := mail.ParseAddress(remetente); err != nil {
		return nil, fmt.Errorf("configure PROMOCOES_EMAIL_FROM com um remetente autorizado")
	}
	return &clienteEmail{
		apiKey:   apiKey,
		endpoint: "https://api.resend.com/emails",
		http:     &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (c *clienteEmail) enviar(ctx context.Context, n notificacao) error {
	body, _ := json.Marshal(n.Email)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "promocao/"+n.ID)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("API de e-mail indisponível: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("API de e-mail retornou HTTP %d", resp.StatusCode)
	}
	return nil
}

// logica de negocio

type inscricao struct {
	Ultimo     base.InteressePromocaoPayload
	Categorias []string
}

type mensagemEmail struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}

type notificacao struct {
	ID        string
	Categoria string
	Email     mensagemEmail
	ExpiraEm  time.Time
}

type servicoPromocoes struct {
	mu         sync.Mutex
	remetente  string
	inscricoes map[string]inscricao
	pendentes  map[string]notificacao
	enviar     func(context.Context, notificacao) error
}

func novoServicoPromocoes(remetente string) *servicoPromocoes {
	return &servicoPromocoes{
		remetente:  remetente,
		inscricoes: make(map[string]inscricao),
		pendentes:  make(map[string]notificacao),
	}
}

func normalizarInteresse(i base.InteressePromocaoPayload) (base.InteressePromocaoPayload, error) {
	i.Email = strings.ToLower(strings.TrimSpace(i.Email))
	if !strings.Contains(i.Email, "@") {
		return i, fmt.Errorf("%w: email inválido", rabbitmq.ErrEventoInvalido)
	}
	i.Categorias = slices.Clone(i.Categorias)
	for n, c := range i.Categorias {
		i.Categorias[n] = strings.ToUpper(strings.TrimSpace(c))
	}
	slices.Sort(i.Categorias)
	i.Categorias = slices.Compact(i.Categorias)
	return i, nil
}

func lerInteresse(d amqp.Delivery) (base.InteressePromocaoPayload, error) {
	var i base.InteressePromocaoPayload
	e, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return i, err
	}
	if err := base.DesserializarPayload(e, &i); err != nil {
		return i, err
	}
	return normalizarInteresse(i)
}

func (s *servicoPromocoes) atualizarInteresse(i base.InteressePromocaoPayload) error {
	i, err := normalizarInteresse(i)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	anterior := s.inscricoes[i.Email]
	if i.Versao < anterior.Ultimo.Versao {
		return nil // Ignora evento antigo
	}

	categorias := slices.Clone(i.Categorias)
	if i.Acao == "cancelar" {
		categorias = nil
		if len(i.Categorias) > 0 { // Cancelamento parcial
			for _, c := range anterior.Categorias {
				if !slices.Contains(i.Categorias, c) {
					categorias = append(categorias, c)
				}
			}
		}
	}

	// Salva na memória
	s.inscricoes[i.Email] = inscricao{Ultimo: i, Categorias: categorias}

	// Limpa envios pendentes se o usuário acabou de cancelar a categoria
	for id, n := range s.pendentes {
		if n.Email.To[0] == i.Email && !slices.Contains(categorias, n.Categoria) {
			delete(s.pendentes, id)
		}
	}
	return nil
}

func (s *servicoPromocoes) agendar(p Promocao) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for email, i := range s.inscricoes {
		if !slices.Contains(i.Categorias, p.Categoria) {
			continue // Usuário não quer essa categoria
		}

		var aleatorio [16]byte
		crand.Read(aleatorio[:])
		id := hex.EncodeToString(aleatorio[:])

		s.pendentes[id] = notificacao{
			ID: id, Categoria: p.Categoria, ExpiraEm: time.Now().UTC().Add(23 * time.Hour),
			Email: mensagemEmail{
				From:    s.remetente,
				To:      []string{email},
				Subject: fmt.Sprintf("Promoção: %s com %d%% de desconto", p.Produto, p.Desconto),
				Text:    fmt.Sprintf("%s está com %d%% de desconto! Categoria: %s.", p.Produto, p.Desconto, p.Categoria),
			},
		}
	}
}

func (s *servicoPromocoes) enviarPendente(ctx context.Context, id string) {
	s.mu.Lock()
	n, ok := s.pendentes[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	expirada := !time.Now().Before(n.ExpiraEm)
	ativa := slices.Contains(s.inscricoes[n.Email.To[0]].Categorias, n.Categoria)
	s.mu.Unlock()

	if !expirada && ativa {
		if err := s.enviar(ctx, n); err != nil {
			log.Printf("[PROMOÇÕES] Falha ao enviar para %s: %v", n.Email.To[0], err)
			return // Se falhar, continua na lista para tentar depois
		}
	}

	// Se enviou com sucesso ou expirou, remove da memória
	s.mu.Lock()
	delete(s.pendentes, id)
	s.mu.Unlock()
}

func (s *servicoPromocoes) processarPendentes(ctx context.Context) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.pendentes))
	for id := range s.pendentes {
		ids = append(ids, id)
	}
	s.mu.Unlock()

	for _, id := range ids {
		s.enviarPendente(ctx, id)
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

// executável principal

type Promocao struct {
	Produto   string `json:"produto"`
	Desconto  int    `json:"desconto"`
	Categoria string `json:"categoria"`
}

var Catalogo = map[string]string{
	"Elden Ring":               "A",
	"Grand Theft Auto V":       "A",
	"Minecraft":                "B",
	"Stardew Valley":           "B",
	"The Witcher 3: Wild Hunt": "C",
	"Red Dead Redemption 2":    "A",
	"Counter-Strike 2":         "C",
	"Baldur's Gate 3":          "B",
}

func sortearPromocao() Promocao {
	produtos := make([]string, 0, len(Catalogo))
	for p := range Catalogo {
		produtos = append(produtos, p)
	}
	produtoSorteado := produtos[mrand.Intn(len(produtos))]
	return Promocao{
		Produto:   produtoSorteado,
		Desconto:  mrand.Intn(40) + 10,
		Categoria: Catalogo[produtoSorteado],
	}
}

func main() {
	intervalo := 5 * time.Minute
	if v := os.Getenv("PROMOCOES_INTERVALO"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			intervalo = d
		}
	}

	remetente := os.Getenv("PROMOCOES_EMAIL_FROM")
	emailClient, err := novoClienteEmail(os.Getenv("RESEND_API_KEY"), remetente)
	if err != nil {
		log.Fatal(err)
	}

	servico := novoServicoPromocoes(remetente)
	servico.enviar = emailClient.enviar

	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	publicador := rabbitmq.AbrirCanal(conn)
	defer publicador.Close()

	rabbitmq.DeclararExchangeEcommerce(ch)
	rabbitmq.DeclararExchangePromocoes(publicador)
	fila := rabbitmq.DeclararFila(ch, "fila.promocoes.interesses")
	rabbitmq.VincularFila(ch, fila.Name, base.InteressePromocao, rabbitmq.ExchangeEcommerce)

	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		interesse, err := lerInteresse(d)
		if err != nil {
			return err
		}
		return servico.atualizarInteresse(interesse)
	})

	ctx := context.Background()
	go servico.repetirPendentes(ctx)

	log.Printf("[PROMOÇÕES] Rodando em RAM; novas ofertas a cada %s", intervalo)
	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()

	for range ticker.C {
		promocao := sortearPromocao()
		log.Printf("[PROMOÇÕES] Nova promoção gerada: %s (%d%%) | categoria %s", promocao.Produto, promocao.Desconto, promocao.Categoria)
		servico.agendar(promocao)

		routingKey := "promocao.categoria." + promocao.Categoria
		evento, err := base.CriarEvento(routingKey, "promocoes", promocao)
		if err != nil {
			continue
		}
		if err := base.AssinarEvento(&evento, "promocoes/keys/promocoes_private.pem"); err != nil {
			continue
		}
		body, err := base.SerializarEvento(evento)
		if err != nil {
			continue
		}
		if err := rabbitmq.PublicarEvento(publicador, rabbitmq.ExchangePromocoes, routingKey, body); err != nil {
			log.Printf("[PROMOÇÕES] Falha ao publicar oferta: %v", err)
		}
	}
}
