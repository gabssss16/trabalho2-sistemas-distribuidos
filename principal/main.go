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

// structs para requisição e resposta REST
type CriarPedidoRequest struct {
	Itens []base.OrderItem `json:"itens"`
}

type PedidoResponse struct {
	OrderID int               `json:"order_id"`
	Status  string            `json:"status"`
	Links   map[string]string `json:"_links"` // Padrão HATEOAS
}

// estrutura pra receber o JSON de interesse em promoções 
type InteresseRequest struct {
	Email      string   `json:"email"`
	Categorias []string `json:"categorias"` // Ex: ["A", "B"]
}

var produtores = map[string]string{
	base.EstoqueOK:           "estoque",
	base.EstoqueIndisponivel: "estoque",
	base.PagamentoAprovado:   "pagamento",
	base.PagamentoRecusado:   "pagamento",
	base.PedidoEnviado:       "entrega",
}

var (
	pedidos   = make(map[int]Pedido)
	proximoID = 1
	mu        sync.Mutex
	canalAMQP *amqp.Channel // var global para o servidor web conseguir publicar mensagens
	clientesSSE = make(map[int]chan string) // Mapa: OrderID -> Canal de mensagens de status
	muSSE       sync.Mutex    
)

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	canalAMQP = rabbitmq.AbrirCanal(conn)
	defer canalAMQP.Close()

	rabbitmq.DeclararExchangeEcommerce(canalAMQP)
	ConfigurarFilaPrincipal(canalAMQP)
	
	// consumidor rodando em background 
	rabbitmq.IniciarConsumo(canalAMQP, FilaPrincipal, func(delivery amqp.Delivery) {
		TratarEvento(canalAMQP, delivery)
	})

	// config do servidor rest
	http.HandleFunc("/pedidos", gerenciarPedidos)
	http.HandleFunc("/sse/pedidos/", handleSSE)
	http.HandleFunc("/produtos", handleListarProdutos)
	http.HandleFunc("POST /pedidos/{id}/cancelar", handleCancelarPedido)
	http.HandleFunc("POST /interesse", handleRegistrarInteresse)
	http.HandleFunc("DELETE /interesse", handleCancelarInteresse)

	log.Println("[*] API Gateway (MS Principal) rodando na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil)) // Fica escutando requisições infinitamente
}

//endpoints rest
func gerenciarPedidos(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

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
	
	// Publica no RabbitMQ reaproveitando sua função original
	if err := PublicarEventoPrincipal(canalAMQP, base.PedidoCriado, payload); err != nil {
		mu.Unlock()
		http.Error(w, "Falha ao processar pedido", http.StatusInternalServerError)
		return
	}

	pedidos[idAtual] = Pedido{Itens: req.Itens, Status: base.PedidoCriado}
	proximoID++
	mu.Unlock()

	// Montando a resposta com HATEOAS
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
		log.Printf("Evento descartado: %v", err)
		return
	}
	produtor, conhecido := produtores[evento.Type]
	if !conhecido || evento.Type != delivery.RoutingKey || evento.Producer != produtor {
		log.Print("Evento descartado: tipo ou produtor incorreto")
		return
	}
	valida, err := base.ValidarAssinatura(evento, "principal/keys/"+produtor+"_public.pem")
	if err != nil || !valida {
		log.Printf("Evento descartado: assinatura inválida ou chave indisponível (%v)", err)
		return
	}
	var payload base.OrderPayload
	if err := base.DesserializarPayload(evento, &payload); err != nil {
		log.Printf("Evento descartado: %v", err)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	pedido, existe := pedidos[payload.OrderID]
	if !existe || pedidoFinalizado(pedido.Status) {
		return
	}
	// Eventos de produtores diferentes podem chegar fora de ordem.
	if evento.Type == base.EstoqueOK && pedido.Status == base.PagamentoAprovado {
		return
	}
	if evento.Type == base.EstoqueIndisponivel || evento.Type == base.PagamentoRecusado {
		// Usa os itens originais, mesmo quando a resposta contém apenas o ID.
		payload.Items = pedido.Itens
		if err := PublicarEventoPrincipal(ch, base.PedidoExcluido, payload); err != nil {
			log.Printf("Falha ao excluir pedido: %v", err)
			return
		}
	}
	pedido.Status = evento.Type
	pedidos[payload.OrderID] = pedido
	fmt.Printf("\nPedido %d: %s\n", payload.OrderID, pedido.Status)

	muSSE.Lock()
	if canal, online := clientesSSE[payload.OrderID]; online {
		canal <- pedido.Status // Joga o novo status dentro do canal transmissor
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
	rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, tipo, body)
	return nil
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	// Pega o ID do pedido na URL (ex: /sse/pedidos/7)
	idStr := strings.TrimPrefix(r.URL.Path, "/sse/pedidos/")
	orderID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "ID inválido", http.StatusBadRequest)
		return
	}

	// Cabeçalhos OBRIGATÓRIOS para o navegador entender que é um streaming de texto, e não um site normal
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Cria o "rádio transmissor" (um canal do Go) para este cliente específico
	msgChan := make(chan string)

	muSSE.Lock()
	clientesSSE[orderID] = msgChan
	muSSE.Unlock()

	// Quando o cliente fechar a aba do navegador, limpa a memória
	defer func() {
		muSSE.Lock()
		delete(clientesSSE, orderID)
		muSSE.Unlock()
		close(msgChan)
	}()

	// Manda o status atual imediatamente quando ele conecta
	mu.Lock()
	if pedido, existe := pedidos[orderID]; existe {
		fmt.Fprintf(w, "data: %s\n\n", pedido.Status)
		w.(http.Flusher).Flush() // Empurra fisicamente os dados pelo cabo da internet na hora
	}
	mu.Unlock()

	// Loop infinito esperando o RabbitMQ avisar de mudanças
	for {
		select {
		case novoStatus := <-msgChan:
			// Recebeu novidade do RabbitMQ! Empurra pro navegador.
			fmt.Fprintf(w, "data: %s\n\n", novoStatus)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			// Cliente cancelou a requisição (fechou o site)
			return
		}
	}
}

func handleListarProdutos(w http.ResponseWriter, r *http.Request) {
	// Libera o CORS para o Frontend conseguir acessar
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		return
	}

	// Faz a requisição REST HTTP GET para o microsserviço de Estoque (que roda na porta 8081)
	resp, err := http.Get("http://localhost:8081/produtos")
	if err != nil {
		log.Printf("[GATEWAY] Falha ao consultar MS Estoque: %v", err)
		http.Error(w, "Serviço de estoque indisponível", http.StatusServiceUnavailable) // Erro 503
		return
	}
	defer resp.Body.Close()

	// Repassa o mesmo Status Code que o Estoque respondeu (ex: 200 OK)
	w.WriteHeader(resp.StatusCode)

	// Copia o corpo da resposta (o JSON inteiro com os produtos) diretamente para o Frontend
	io.Copy(w, resp.Body)
}

func handleCancelarPedido(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Pega o ID da URL (recurso do Go 1.22+)
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
		http.Error(w, "Pedido já enviado ou cancelado", http.StatusConflict)
		return
	}

	// Monta o evento de exclusão e publica no RabbitMQ
	payload := base.OrderPayload{OrderID: orderID, Items: pedido.Itens}
	if err := PublicarEventoPrincipal(canalAMQP, base.PedidoExcluido, payload); err != nil {
		mu.Unlock()
		http.Error(w, "Falha ao publicar cancelamento", http.StatusInternalServerError)
		return
	}

	// Atualiza a memória
	pedido.Status = base.PedidoExcluido
	pedidos[orderID] = pedido
	mu.Unlock()

	// Avisa o cliente no SSE
	muSSE.Lock()
	if canal, online := clientesSSE[orderID]; online {
		canal <- base.PedidoExcluido
	}
	muSSE.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "Pedido %d cancelado com sucesso"}`, orderID)
}

func handleRegistrarInteresse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	var req InteresseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	// Usa o tempo atual como versão (milissegundos) para garantir a ordem dos eventos
	payload := base.InteressePromocaoPayload{
		Email:      req.Email,
		Categorias: req.Categorias,
		Acao:       "cadastrar",
		Versao:     time.Now().UnixMilli(), 
	}

	if err := PublicarInteressePrincipal(canalAMQP, payload); err != nil {
		http.Error(w, "Falha ao registrar interesse", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"message": "Interesse cadastrado com sucesso"}`)
}

func handleCancelarInteresse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	var req InteresseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	payload := base.InteressePromocaoPayload{
		Email:      req.Email,
		Categorias: req.Categorias, // Pode ser vazio se for cancelar tudo, ou específico para cancelar uma só
		Acao:       "cancelar",
		Versao:     time.Now().UnixMilli(),
	}

	if err := PublicarInteressePrincipal(canalAMQP, payload); err != nil {
		http.Error(w, "Falha ao cancelar interesse", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "Interesse cancelado com sucesso"}`)
}

// PublicarInteressePrincipal assina e publica especificamente o payload de promoções
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
	rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, base.InteressePromocao, body)
	return nil
}