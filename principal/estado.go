package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/persistencia"
)

type Pedido struct {
	Itens     []base.OrderItem `json:"itens"`
	Status    string           `json:"status"`
	Historico []string         `json:"historico"`
}
type interesseSalvo struct {
	Versao     int64    `json:"versao"`
	Categorias []string `json:"categorias"`
}
type pendente struct {
	ID      int64           `json:"id"`
	Tipo    string          `json:"tipo"`
	Payload json.RawMessage `json:"payload"`
}
type estadoPrincipal struct {
	ProximoID     int                       `json:"proximo_id"`
	ProximoEvento int64                     `json:"proximo_evento"`
	Pedidos       map[int]Pedido            `json:"pedidos"`
	Interesses    map[string]interesseSalvo `json:"interesses"`
	Pendentes     []pendente                `json:"pendentes"`
}
type servicoPrincipal struct {
	mu                       sync.Mutex
	envioMu                  sync.Mutex
	caminho                  string
	estado                   estadoPrincipal
	clientes                 map[int]map[chan struct{}]struct{}
	acordar                  chan struct{}
	publicar                 func(string, any) error
	cliente                  *http.Client
	estoqueURL, pagamentoURL string
}

func abrirPrincipal(caminho string) (*servicoPrincipal, error) {
	s := &servicoPrincipal{caminho: caminho, clientes: make(map[int]map[chan struct{}]struct{}), acordar: make(chan struct{}, 1), cliente: &http.Client{Timeout: 5 * time.Second}, estoqueURL: "http://localhost:8081", pagamentoURL: "http://localhost:8082"}
	b, err := os.ReadFile(caminho)
	if errors.Is(err, os.ErrNotExist) {
		// O primeiro ID fica fora da faixa 1,2,3 usada pela demonstracao antiga.
		s.estado = estadoPrincipal{ProximoID: int(time.Now().UnixMilli()), ProximoEvento: 1, Pedidos: make(map[int]Pedido), Interesses: make(map[string]interesseSalvo)}
		return s, persistencia.Salvar(caminho, s.estado)
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &s.estado); err != nil {
		return nil, err
	}
	e := s.estado
	if e.ProximoID <= 0 || e.ProximoEvento <= 0 || e.Pedidos == nil || e.Interesses == nil {
		return nil, fmt.Errorf("estado do Principal inválido")
	}
	for id, p := range e.Pedidos {
		if id <= 0 || id >= e.ProximoID || validarItens(p.Itens) != nil || len(p.Historico) == 0 || p.Historico[len(p.Historico)-1] != p.Status {
			return nil, fmt.Errorf("pedido persistido inválido: %d", id)
		}
		for _, st := range p.Historico {
			if !statusConhecido(st) {
				return nil, fmt.Errorf("status persistido inválido")
			}
		}
	}
	var anterior int64
	for _, p := range e.Pendentes {
		if p.ID <= anterior || p.ID >= e.ProximoEvento || !json.Valid(p.Payload) {
			return nil, fmt.Errorf("publicação pendente inválida")
		}
		anterior = p.ID
		switch p.Tipo {
		case base.PedidoCriado, base.PedidoExcluido, base.PedidoCancelamentoSolicitado, base.InteressePromocao:
		default:
			return nil, fmt.Errorf("tipo pendente inválido")
		}
	}
	for email, i := range e.Interesses {
		acao := "cadastrar"
		if len(i.Categorias) == 0 {
			acao = "cancelar"
		}
		normal, err := base.NormalizarInteresse(base.InteressePromocaoPayload{Email: email, Categorias: i.Categorias, Acao: acao, Versao: i.Versao})
		if err != nil || normal.Email != email || !slices.Equal(normal.Categorias, i.Categorias) {
			return nil, fmt.Errorf("interesse persistido inválido")
		}
	}
	return s, nil
}

// Chamado sob mu. As colecoes alteradas sao copiadas antes de gravar.
func (s *servicoPrincipal) copiarEstado() estadoPrincipal {
	e := s.estado
	e.Pedidos = make(map[int]Pedido, len(s.estado.Pedidos))
	for id, p := range s.estado.Pedidos {
		p.Historico = slices.Clone(p.Historico)
		e.Pedidos[id] = p
	}
	e.Interesses = make(map[string]interesseSalvo, len(s.estado.Interesses))
	for k, v := range s.estado.Interesses {
		v.Categorias = slices.Clone(v.Categorias)
		e.Interesses[k] = v
	}
	e.Pendentes = slices.Clone(e.Pendentes)
	return e
}
func (s *servicoPrincipal) salvar(e estadoPrincipal) error {
	if err := persistencia.Salvar(s.caminho, e); err != nil {
		return err
	}
	s.estado = e
	return nil
}
func (e *estadoPrincipal) agendar(tipo string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	e.Pendentes = append(e.Pendentes, pendente{e.ProximoEvento, tipo, b})
	e.ProximoEvento++
	return nil
}
func (s *servicoPrincipal) sinalizar() {
	select {
	case s.acordar <- struct{}{}:
	default:
	}
}

// Serializa publicacoes, mas nunca segura mu durante acesso ao broker.
func (s *servicoPrincipal) retomarPendentes() error {
	s.envioMu.Lock()
	defer s.envioMu.Unlock()
	for {
		s.mu.Lock()
		if len(s.estado.Pendentes) == 0 {
			s.mu.Unlock()
			return nil
		}
		p := s.estado.Pendentes[0]
		s.mu.Unlock()
		if err := s.publicar(p.Tipo, p.Payload); err != nil {
			return err
		}
		s.mu.Lock()
		e := s.copiarEstado()
		e.Pendentes = e.Pendentes[1:]
		err := s.salvar(e)
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
}
func (s *servicoPrincipal) republicarPendentes(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.retomarPendentes(); err != nil {
			log.Printf("[PRINCIPAL] Publicação pendente: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.acordar:
		}
	}
}
func pedidoFinalizado(st string) bool {
	return st == base.PedidoExcluido || st == base.EstoqueIndisponivel || st == base.PagamentoRecusado || st == base.PedidoEnviado
}
func statusConhecido(st string) bool {
	return st == base.PedidoCriado || st == base.PedidoCancelamentoSolicitado || st == base.EstoqueOK || st == base.PagamentoAprovado || pedidoFinalizado(st)
}
func validarItens(itens []base.OrderItem) error {
	if len(itens) == 0 {
		return fmt.Errorf("informe ao menos um item")
	}
	quantidades := make(map[int]int)
	for _, i := range itens {
		if i.ProductID <= 0 || i.Quantity <= 0 || quantidades[i.ProductID] > math.MaxInt-i.Quantity {
			return fmt.Errorf("produto/quantidade inválidos ou quantidade excede limite")
		}
		quantidades[i.ProductID] += i.Quantity
	}
	return nil
}
func (s *servicoPrincipal) avisar(id int) {
	for c := range s.clientes[id] {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
