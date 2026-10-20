package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	servico, err := abrirPagamento(
		config("MOCK_PAGAMENTO_URL", "http://localhost:8083/cobrancas"),
		config("PAGAMENTO_WEBHOOK_URL", "http://localhost:8082/webhook"),
	)
	if err != nil {
		log.Fatal(err)
	}
	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	rabbitmq.DeclararTopologiaEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila.pagamento")
	rabbitmq.VincularFila(ch, fila.Name, base.EstoqueOK, rabbitmq.ExchangeEcommerce)
	rabbitmq.VincularFila(ch, fila.Name, base.PedidoCancelamentoSolicitado, rabbitmq.ExchangeEcommerce)
	servico.publicar = rabbitmq.NovoPublicadorAssinado(conn, "pagamento", "pagamento/keys/pagamento_private.pem")
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		if d.RoutingKey == base.PedidoCancelamentoSolicitado {
			pedido, err := lerPedidoOrigem(d, base.PedidoCancelamentoSolicitado, "principal")
			if err != nil {
				return err
			}
			return servico.cancelarPedido(pedido)
		}
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
	return lerPedidoOrigem(d, base.EstoqueOK, "estoque")
}
func lerPedidoOrigem(d amqp.Delivery, tipo, produtor string) (base.OrderPayload, error) {
	var pedido base.OrderPayload
	evento, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return pedido, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if evento.Type != tipo || d.RoutingKey != tipo || evento.Producer != produtor {
		return pedido, fmt.Errorf("%w: esperado pedido.estoque_ok do estoque", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(evento, "pagamento/keys/"+produtor+"_public.pem")
	if err != nil || !ok {
		return pedido, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	if pedido.OrderID <= 0 || len(pedido.Items) == 0 {
		return pedido, fmt.Errorf("%w: pedido sem identificador ou itens", rabbitmq.ErrEventoInvalido)
	}
	for _, item := range pedido.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 {
			return pedido, fmt.Errorf("%w: item inválido", rabbitmq.ErrEventoInvalido)
		}
	}
	return pedido, nil
}

func config(nome, padrao string) string {
	if valor := os.Getenv(nome); valor != "" {
		return valor
	}
	return padrao
}

type cobranca struct {
	Pedido      base.OrderPayload `json:"pedido"`
	ChargeID    string            `json:"charge_id"`
	CheckoutURL string            `json:"checkout_url"`
	Status      string            `json:"status"`
	Publicado   bool              `json:"published"`
}

type servicoPagamento struct {
	mu                  sync.Mutex
	mockURL, webhookURL string
	cliente             *http.Client
	cobrancas           map[int]cobranca
	publicar            func(string, base.OrderPayload) error
}

func abrirPagamento(mockURL, webhookURL string) (*servicoPagamento, error) {
	if !urlHTTP(mockURL) || !urlHTTP(webhookURL) {
		return nil, fmt.Errorf("URLs do mock e webhook devem ser HTTP(S) absolutas")
	}
	s := &servicoPagamento{
		mockURL: mockURL, webhookURL: webhookURL,
		cliente: &http.Client{Timeout: 10 * time.Second}, cobrancas: make(map[int]cobranca),
	}
	return s, nil
}

func urlHTTP(valor string) bool {
	u, err := url.Parse(valor)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil
}

func (s *servicoPagamento) criarCobranca(pedido base.OrderPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existente, ok := s.cobrancas[pedido.OrderID]; ok {
		if !pedidosIguais(existente.Pedido, pedido) {
			return fmt.Errorf("%w: order_id reutilizado", rabbitmq.ErrEventoInvalido)
		}
		return nil
	}
	requisicao := struct {
		base.OrderPayload
		WebhookURL string `json:"webhook_url"`
	}{pedido, s.webhookURL}
	body, err := json.Marshal(requisicao)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.mockURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// O mock deve reutilizar a cobrança para esta chave em tentativas posteriores.
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&resposta); err != nil {
		return fmt.Errorf("resposta do mock: %w", err)
	}
	if resposta.ChargeID == "" || !urlHTTP(resposta.CheckoutURL) {
		return fmt.Errorf("mock não informou charge_id e checkout_url válidos")
	}
	c := cobranca{Pedido: pedido, ChargeID: resposta.ChargeID, CheckoutURL: resposta.CheckoutURL, Status: "PENDENTE"}
	s.cobrancas[pedido.OrderID] = c
	log.Printf("[PAGAMENTO] Pedido %d: checkout %s", pedido.OrderID, c.CheckoutURL)
	return nil
}

func pedidosIguais(a, b base.OrderPayload) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func (s *servicoPagamento) rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", s.receberWebhook)
	// O Gateway pode consultar a URL após a criação assíncrona da cobrança.
	mux.HandleFunc("GET /pagamentos/{order_id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("order_id"))
		if err != nil || id <= 0 {
			responderErro(w, 400, "order_id inválido")
			return
		}
		s.mu.Lock()
		c, ok := s.cobrancas[id]
		s.mu.Unlock()
		if !ok {
			responderErro(w, 404, "cobrança ainda não disponível")
			return
		}
		responderJSON(w, 200, struct {
			OrderID     int    `json:"order_id"`
			ChargeID    string `json:"charge_id"`
			CheckoutURL string `json:"checkout_url"`
			Status      string `json:"status"`
		}{id, c.ChargeID, c.CheckoutURL, c.Status})
	})
	return mux
}

func (s *servicoPagamento) receberWebhook(w http.ResponseWriter, r *http.Request) {
	var notificacao struct {
		OrderID  int    `json:"order_id"`
		ChargeID string `json:"charge_id"`
		Status   string `json:"status"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&notificacao); err != nil {
		responderErro(w, 400, "JSON inválido")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		responderErro(w, 400, "esperado um único objeto JSON")
		return
	}
	tipo := ""
	switch notificacao.Status {
	case "APROVADO":
		tipo = base.PagamentoAprovado
	case "RECUSADO":
		tipo = base.PagamentoRecusado
	default:
		responderErro(w, 400, "status deve ser APROVADO ou RECUSADO")
		return
	}
	if notificacao.OrderID <= 0 || notificacao.ChargeID == "" {
		responderErro(w, 400, "order_id e charge_id são obrigatórios")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cobrancas[notificacao.OrderID]
	if !ok {
		responderErro(w, 404, "cobrança não encontrada")
		return
	}
	if c.ChargeID != notificacao.ChargeID {
		responderErro(w, 409, "charge_id não corresponde ao pedido")
		return
	}
	if c.Status != "PENDENTE" && c.Status != notificacao.Status {
		responderErro(w, 409, "cobrança já finalizada com outro status")
		return
	}
	if c.Publicado {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Guarda a decisão para rejeitar resultados conflitantes e permitir repetir o webhook.
	c.Status = notificacao.Status
	s.cobrancas[c.Pedido.OrderID] = c
	if err := s.publicarResultado(c, tipo); err != nil {
		log.Printf("[PAGAMENTO] Falha ao publicar pedido %d: %v", c.Pedido.OrderID, err)
		responderErro(w, 503, "falha ao publicar resultado; repita o webhook")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func responderJSON(w http.ResponseWriter, status int, valor any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(valor)
}

func responderErro(w http.ResponseWriter, status int, mensagem string) {
	responderJSON(w, status, map[string]string{"error": mensagem})
}

// Chamado sob mutex; evita republicar um resultado já confirmado.
func (s *servicoPagamento) publicarResultado(c cobranca, tipo string) error {
	if c.Publicado {
		return nil
	}
	if err := s.publicar(tipo, c.Pedido); err != nil {
		return err
	}
	c.Publicado = true
	s.cobrancas[c.Pedido.OrderID] = c
	return nil
}

// Pagamento arbitra cancelamento e webhook sob o mesmo mutex. Quem finalizar
// primeiro decide; uma aprovacao existente nunca libera estoque como cancelada.
func (s *servicoPagamento) cancelarPedido(pedido base.OrderPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cobrancas[pedido.OrderID]
	if ok && !pedidosIguais(c.Pedido, pedido) {
		return fmt.Errorf("%w: pedido divergente", rabbitmq.ErrEventoInvalido)
	}
	if !ok {
		c = cobranca{Pedido: pedido, ChargeID: "cancelado-" + strconv.Itoa(pedido.OrderID), Status: "PENDENTE"}
	}
	if c.Status == "PENDENTE" {
		c.Status = "RECUSADO"
		c.Publicado = false
		s.cobrancas[pedido.OrderID] = c
	}
	tipo := base.PagamentoRecusado
	if c.Status == "APROVADO" {
		tipo = base.PagamentoAprovado
	}
	return s.publicarResultado(c, tipo)
}
