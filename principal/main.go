package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

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
