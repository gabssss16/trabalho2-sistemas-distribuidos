package main

import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

// Simulando o banco de dados do estoque (ID do produto -> Quantidade disponível)
var (
	estoque = map[int]int{
		1: 15, // Produto 1
		2: 10, // Produto 2
		3: 5,  // Produto 3
	}
	mu sync.Mutex // Protege o estoque contra acessos simultâneos
)

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	rabbitmq.DeclararExchangeEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila_estoque")

	// O Estoque escuta a criação e a exclusão de pedidos
	rabbitmq.VincularFila(ch, fila.Name, base.PedidoCriado, rabbitmq.ExchangeEcommerce)
	rabbitmq.VincularFila(ch, fila.Name, base.PedidoExcluido, rabbitmq.ExchangeEcommerce)

	handler := func(d amqp.Delivery) {
		evento, err := base.DesserializarEvento(d.Body)
		if err != nil {
			log.Printf("[ESTOQUE - ERRO] Falha ao ler envelope: %v", err)
			return
		}

		// Valida a assinatura digital usando a chave pública do MS Principal
		valido, err := base.ValidarAssinatura(evento, "estoque/keys/principal_public.pem")
		if err != nil || !valido {
			log.Printf("[ESTOQUE - ALERTA] Evento descartado! Assinatura inválida: %v", err)
			return
		}

		var payload base.OrderPayload
		if err := base.DesserializarPayload(evento, &payload); err != nil {
			log.Printf("[ESTOQUE - ERRO] Falha ao extrair payload: %v", err)
			return
		}

		// Processa o evento com segurança de concorrência
		mu.Lock()
		if evento.Type == base.PedidoCriado {
			processarPedidoCriado(ch, payload)
		} else if evento.Type == base.PedidoExcluido {
			processarPedidoExcluido(payload)
		}
		mu.Unlock()
	}

	log.Println("[*] MS Estoque rodando e aguardando pedidos... CTRL+C para sair")
	rabbitmq.IniciarConsumo(ch, fila.Name, handler)

	var forever chan struct{}
	<-forever
}

func processarPedidoCriado(ch *amqp.Channel, payload base.OrderPayload) {
	temEstoque := true

	// Verifica se existe quantidade suficiente de TODOS os itens solicitados
	for _, item := range payload.Items {
		if estoque[item.ProductID] < item.Quantity {
			temEstoque = false
			break
		}
	}

	var routingKey string
	if temEstoque {
		// Deduz as quantidades do estoque físico
		for _, item := range payload.Items {
			estoque[item.ProductID] -= item.Quantity
		}
		routingKey = base.EstoqueOK
		log.Printf("[ESTOQUE] Pedido %d APROVADO. Estoque atualizado.", payload.OrderID)
	} else {
		routingKey = base.EstoqueIndisponivel
		log.Printf("[ESTOQUE] Pedido %d RECUSADO. Falta de estoque.", payload.OrderID)
	}

	publicarResposta(ch, routingKey, payload)
}

func processarPedidoExcluido(payload base.OrderPayload) {
	// Devolve os produtos para o estoque
	for _, item := range payload.Items {
		estoque[item.ProductID] += item.Quantity
	}
	log.Printf("[ESTOQUE] Pedido %d EXCLUÍDO. Itens devolvidos.", payload.OrderID)
}

func publicarResposta(ch *amqp.Channel, routingKey string, payload base.OrderPayload) {
	// 1. Empacota
	evento, err := base.CriarEvento(routingKey, "estoque", payload)
	if err != nil {
		log.Printf("[ESTOQUE - ERRO] Falha ao criar evento: %v", err)
		return
	}

	// 2. Assina com a chave privada do Estoque
	if err := base.AssinarEvento(&evento, "estoque/keys/estoque_private.pem"); err != nil {
		log.Printf("[ESTOQUE - ERRO] Falha ao assinar evento: %v", err)
		return
	}

	// 3. Serializa e publica
	body, err := base.SerializarEvento(evento)
	if err != nil {
		log.Printf("[ESTOQUE - ERRO] Falha ao serializar evento: %v", err)
		return
	}

	rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, routingKey, body)
}