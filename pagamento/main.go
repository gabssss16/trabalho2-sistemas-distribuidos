package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type cobranca struct {
	Pedido      base.OrderPayload `json:"pedido"`
	ChargeID    string            `json:"charge_id"`
	CheckoutURL string            `json:"checkout_url"`
	Status      string            `json:"status"`
	Publicado   bool              `json:"published"`
}

type servicoPagamento struct {
	mu         sync.Mutex
	mockURL    string
	webhookURL string
	cliente    *http.Client
	cobrancas  map[int]cobranca
	publicar   func(string, base.OrderPayload) error
}

func config(nome, padrao string) string {
	if valor := os.Getenv(nome); valor != "" {
		return valor
	}
	return padrao
}

func main() {
	servico := &servicoPagamento{
		mockURL:    config("MOCK_PAGAMENTO_URL", "http://localhost:8083/cobrancas"),
		webhookURL: config("PAGAMENTO_WEBHOOK_URL", "http://localhost:8082/webhook"),
		cliente:    &http.Client{Timeout: 10 * time.Second},
		cobrancas:  make(map[int]cobranca),
	}

	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	rabbitmq.DeclararExchangeEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila.pagamento")
	rabbitmq.VincularFila(ch, fila.Name, base.EstoqueOK, rabbitmq.ExchangeEcommerce)

	servico.publicar = rabbitmq.NovoPublicadorAssinado(conn, "pagamento", "pagamento/keys/pagamento_private.pem")
	
	// Fica tentando republicar avisos que por ventura falharam (apenas na RAM)
	go servico.republicarPendentes(context.Background())

	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		pedido, err := lerPedidoEstoque(d)
		if err != nil {
			return err
		}
		return servico.criarCobranca(pedido)
	})

	addr := config("PAGAMENTO_ADDR", ":8082")
	log.Printf("[PAGAMENTO] HTTP em %s; webhook %s", addr, servico.webhookURL)
	log.Fatal((&http.Server{Addr: addr, Handler: servico.rotas(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}

func lerPedidoEstoque(d amqp.Delivery) (base.OrderPayload, error) {
	var pedido base.OrderPayload
	evento, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return pedido, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if evento.Type != base.EstoqueOK || d.RoutingKey != base.EstoqueOK || evento.Producer != "estoque" {
		return pedido, fmt.Errorf("%w: esperado pedido.estoque_ok do estoque", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(evento, "pagamento/keys/estoque_public.pem")
	if err != nil || !ok {
		return pedido, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	return pedido, nil
}

func (s *servicoPagamento) criarCobranca(pedido base.OrderPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Se já existe na memória RAM, ignora para não criar duplicado no Mock
	if _, ok := s.cobrancas[pedido.OrderID]; ok {
		return nil
	}

	requisicao := struct {
		base.OrderPayload
		WebhookURL string `json:"webhook_url"`
	}{pedido, s.webhookURL}

	body, _ := json.Marshal(requisicao)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.mockURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "pedido-"+strconv.Itoa(pedido.OrderID))

	resp, err := s.cliente.Do(req)
	if err != nil {
		return fmt.Errorf("solicitar cobrança: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("mock retornou HTTP %d", resp.StatusCode)
	}

	var resposta struct {
		ChargeID    string `json:"charge_id"`
		CheckoutURL string `json:"checkout_url"`
	}
	json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&resposta)

	// Guarda exclusivamente na memória RAM
	c := cobranca{Pedido: pedido, ChargeID: resposta.ChargeID, CheckoutURL: resposta.CheckoutURL, Status: "PENDENTE"}
	s.cobrancas[pedido.OrderID] = c
	log.Printf("[PAGAMENTO] Pedido %d: checkout %s", pedido.OrderID, c.CheckoutURL)
	
	return nil
}

func (s *servicoPagamento) rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", s.receberWebhook)
	
	mux.HandleFunc("GET /pagamentos/{order_id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		id, _ := strconv.Atoi(r.PathValue("order_id"))
		
		s.mu.Lock()
		c, ok := s.cobrancas[id]
		s.mu.Unlock()
		
		if !ok {
			http.Error(w, `{"error": "cobrança ainda não disponível"}`, 404)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c)
	})
	return mux
}

func (s *servicoPagamento) receberWebhook(w http.ResponseWriter, r *http.Request) {
	var notificacao struct {
		OrderID  int    `json:"order_id"`
		ChargeID string `json:"charge_id"`
		Status   string `json:"status"`
	}
	json.NewDecoder(r.Body).Decode(&notificacao)

	tipo := ""
	if notificacao.Status == "APROVADO" {
		tipo = base.PagamentoAprovado
	} else if notificacao.Status == "RECUSADO" {
		tipo = base.PagamentoRecusado
	} else {
		http.Error(w, `{"error": "status invalido"}`, 400)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cobrancas[notificacao.OrderID]
	if !ok || c.Publicado {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	c.Status = notificacao.Status
	s.cobrancas[notificacao.OrderID] = c 

	if err := s.publicar(tipo, c.Pedido); err != nil {
		log.Printf("[PAGAMENTO] Falha ao publicar pedido %d", c.Pedido.OrderID)
		return // Vai tentar de novo na rotina de pendentes
	}
	
	c.Publicado = true
	s.cobrancas[notificacao.OrderID] = c
	w.WriteHeader(http.StatusNoContent)
}

func (s *servicoPagamento) republicarPendentes(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			for _, c := range s.cobrancas {
				if c.Publicado || c.Status == "PENDENTE" {
					continue
				}
				tipo := base.PagamentoAprovado
				if c.Status == "RECUSADO" {
					tipo = base.PagamentoRecusado
				}
				if err := s.publicar(tipo, c.Pedido); err == nil {
					c.Publicado = true
					s.cobrancas[c.Pedido.OrderID] = c
				}
			}
			s.mu.Unlock()
		}
	}
}