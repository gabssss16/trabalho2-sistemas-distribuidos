package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

const FilaPrincipal = "fila.principal"

type Pedido struct {
	Itens  []base.OrderItem
	Status string
}

type CriarPedidoRequest struct {
	Itens []base.OrderItem `json:"itens"`
}

type PedidoResponse struct {
	OrderID int               `json:"order_id"`
	Status  string            `json:"status"`
	Links   map[string]string `json:"_links"`
}

type InteresseRequest struct {
	Email      string   `json:"email"`
	Categorias []string `json:"categorias"`
}

var produtores = map[string]string{
	base.EstoqueOK:           "estoque",
	base.EstoqueIndisponivel: "estoque",
	base.PagamentoAprovado:   "pagamento",
	base.PagamentoRecusado:   "pagamento",
	base.PedidoEnviado:       "entrega",
}

var (
	pedidos     = make(map[int]Pedido)
	proximoID   = 1
	mu          sync.Mutex
	canalAMQP   *amqp.Channel
	clientesSSE = make(map[int]chan string)
	muSSE       sync.Mutex
)

// Middleware Universal para liberar o bloqueio de CORS do navegador
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Responde com "OK" imediato para a checagem de segurança do navegador
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	canalAMQP = rabbitmq.AbrirCanal(conn)
	defer canalAMQP.Close()

	rabbitmq.DeclararExchangeEcommerce(canalAMQP)
	ConfigurarFilaPrincipal(canalAMQP)

	rabbitmq.IniciarConsumo(canalAMQP, FilaPrincipal, func(delivery amqp.Delivery) {
		TratarEvento(canalAMQP, delivery)
	})

	// Rotas protegidas pelo CORS (sem métodos na string da URL)
	http.HandleFunc("/pedidos", corsMiddleware(gerenciarPedidos))
	http.HandleFunc("/sse/pedidos/", corsMiddleware(handleSSE))
	http.HandleFunc("/produtos", corsMiddleware(handleListarProdutos))
	http.HandleFunc("/pedidos/{id}/cancelar", corsMiddleware(handleCancelarPedido))
	http.HandleFunc("/interesse", corsMiddleware(handleGerenciarInteresse))

	log.Println("[*] API Gateway (MS Principal) rodando na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleGerenciarInteresse(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		handleRegistrarInteresse(w, r)
	} else if r.Method == http.MethodDelete {
		handleCancelarInteresse(w, r)
	} else {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
	}
}

func gerenciarPedidos(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		handleCriarPedido(w, r)
		return
	}
	http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
}

func handleCriarPedido(w http.ResponseWriter, r *http.Request) {
	var req CriarPedidoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	if len(req.Itens) == 0 {
		http.Error(w, "O pedido deve ter pelo menos um item", http.StatusBadRequest)
		return
	}

	mu.Lock()
	idAtual := proximoID
	payload := base.OrderPayload{OrderID: idAtual, Items: req.Itens}

	if err := PublicarEventoPrincipal(canalAMQP, base.PedidoCriado, payload); err != nil {
		mu.Unlock()
		log.Printf("[PRINCIPAL] Falha ao publicar pedido %d: %v", idAtual, err)
		http.Error(w, "Falha ao processar pedido", http.StatusInternalServerError)
		return
	}

	pedidos[idAtual] = Pedido{Itens: req.Itens, Status: base.PedidoCriado}
	proximoID++
	mu.Unlock()

	resposta := PedidoResponse{
		OrderID: idAtual,
		Status:  base.PedidoCriado,
		Links: map[string]string{
			"self":           fmt.Sprintf("/pedidos/%d", idAtual),
			"cancelar":       fmt.Sprintf("/pedidos/%d/cancelar", idAtual),
			"acompanhar_sse": fmt.Sprintf("/sse/pedidos/%d", idAtual),
		},
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resposta)
}

func ConfigurarFilaPrincipal(ch *amqp.Channel) {
	rabbitmq.DeclararFila(ch, FilaPrincipal)
	for tipo := range produtores {
		rabbitmq.VincularFila(ch, FilaPrincipal, tipo, rabbitmq.ExchangeEcommerce)
	}
}

func TratarEvento(ch *amqp.Channel, delivery amqp.Delivery) {
	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		return
	}
	produtor, conhecido := produtores[evento.Type]
	if !conhecido || evento.Type != delivery.RoutingKey || evento.Producer != produtor {
		return
	}
	valida, err := base.ValidarAssinatura(evento, "principal/keys/"+produtor+"_public.pem")
	if err != nil || !valida {
		return
	}
	var payload base.OrderPayload
	if err := base.DesserializarPayload(evento, &payload); err != nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()
	pedido, existe := pedidos[payload.OrderID]
	if !existe || pedidoFinalizado(pedido.Status) {
		return
	}
	if evento.Type == base.EstoqueOK && pedido.Status == base.PagamentoAprovado {
		return
	}
	if evento.Type == base.EstoqueIndisponivel || evento.Type == base.PagamentoRecusado {
		payload.Items = pedido.Itens
		if err := PublicarEventoPrincipal(ch, base.PedidoExcluido, payload); err != nil {
			return
		}
	}
	pedido.Status = evento.Type
	pedidos[payload.OrderID] = pedido

	muSSE.Lock()
	if canal, online := clientesSSE[payload.OrderID]; online {
		canal <- pedido.Status
	}
	muSSE.Unlock()
}

func pedidoFinalizado(status string) bool {
	return status == base.PedidoExcluido || status == base.EstoqueIndisponivel ||
		status == base.PagamentoRecusado || status == base.PedidoEnviado
}

func PublicarEventoPrincipal(ch *amqp.Channel, tipo string, payload base.OrderPayload) error {
	evento, err := base.CriarEvento(tipo, "principal", payload)
	if err != nil {
		return err
	}
	if err := base.AssinarEvento(&evento, "principal/keys/principal_private.pem"); err != nil {
		return err
	}
	body, err := base.SerializarEvento(evento)
	if err != nil {
		return err
	}
	return rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, tipo, body)
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/sse/pedidos/")
	orderID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "ID inválido", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	msgChan := make(chan string)

	muSSE.Lock()
	clientesSSE[orderID] = msgChan
	muSSE.Unlock()

	defer func() {
		muSSE.Lock()
		delete(clientesSSE, orderID)
		muSSE.Unlock()
		close(msgChan)
	}()

	mu.Lock()
	if pedido, existe := pedidos[orderID]; existe {
		fmt.Fprintf(w, "data: %s\n\n", pedido.Status)
		w.(http.Flusher).Flush()
	}
	mu.Unlock()

	for {
		select {
		case novoStatus := <-msgChan:
			fmt.Fprintf(w, "data: %s\n\n", novoStatus)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func handleListarProdutos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		return
	}
	resp, err := http.Get("http://localhost:8081/produtos")
	if err != nil {
		http.Error(w, "Serviço de estoque indisponível", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func handleCancelarPedido(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		return
	}
	idStr := r.PathValue("id")
	orderID, err := strconv.Atoi(idStr)
	if err != nil || orderID <= 0 {
		http.Error(w, "ID inválido", http.StatusBadRequest)
		return
	}

	mu.Lock()
	pedido, existe := pedidos[orderID]
	if !existe {
		mu.Unlock()
		http.Error(w, "Pedido não encontrado", http.StatusNotFound)
		return
	}
	if pedidoFinalizado(pedido.Status) {
		mu.Unlock()
		http.Error(w, "Pedido já finalizado", http.StatusConflict)
		return
	}

	payload := base.OrderPayload{OrderID: orderID, Items: pedido.Itens}
	if err := PublicarEventoPrincipal(canalAMQP, base.PedidoExcluido, payload); err != nil {
		mu.Unlock()
		log.Printf("[PRINCIPAL] Falha ao publicar cancelamento do pedido %d: %v", orderID, err)
		http.Error(w, "Falha ao cancelar pedido", http.StatusInternalServerError)
		return
	}

	pedido.Status = base.PedidoExcluido
	pedidos[orderID] = pedido
	mu.Unlock()

	muSSE.Lock()
	if canal, online := clientesSSE[orderID]; online {
		canal <- base.PedidoExcluido
	}
	muSSE.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "Cancelado"}`)
}

func handleRegistrarInteresse(w http.ResponseWriter, r *http.Request) {
	var req InteresseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}
	payload := base.InteressePromocaoPayload{
		Email:      req.Email,
		Categorias: req.Categorias,
		Acao:       "cadastrar",
		Versao:     time.Now().UnixMilli(),
	}
	if err := PublicarInteressePrincipal(canalAMQP, payload); err != nil {
		log.Printf("[PRINCIPAL] Falha ao publicar interesse de promoção: %v", err)
		http.Error(w, "Falha ao processar interesse", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"message": "Cadastrado"}`)
}

func handleCancelarInteresse(w http.ResponseWriter, r *http.Request) {
	var req InteresseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}
	payload := base.InteressePromocaoPayload{
		Email:      req.Email,
		Categorias: req.Categorias,
		Acao:       "cancelar",
		Versao:     time.Now().UnixMilli(),
	}
	if err := PublicarInteressePrincipal(canalAMQP, payload); err != nil {
		log.Printf("[PRINCIPAL] Falha ao publicar cancelamento de interesse: %v", err)
		http.Error(w, "Falha ao cancelar interesse", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "Cancelado"}`)
}

func PublicarInteressePrincipal(ch *amqp.Channel, payload base.InteressePromocaoPayload) error {
	evento, err := base.CriarEvento(base.InteressePromocao, "principal", payload)
	if err != nil {
		return err
	}
	if err := base.AssinarEvento(&evento, "principal/keys/principal_private.pem"); err != nil {
		return err
	}
	body, err := base.SerializarEvento(evento)
	if err != nil {
		return err
	}
	return rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, base.InteressePromocao, body)
}
