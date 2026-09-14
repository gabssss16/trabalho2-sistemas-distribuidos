package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	rabbitmq.DeclararExchangeEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila.entrega")
	rabbitmq.VincularFila(ch, fila.Name, base.PagamentoAprovado, rabbitmq.ExchangeEcommerce)

	rabbitmq.IniciarConsumo(ch, fila.Name, func(delivery amqp.Delivery) {
		resposta, err := ProcessarEntrega(delivery)
		if err != nil {
			log.Printf("[ENTREGA] Evento não processado: %v", err)
			return
		}
		body, err := base.SerializarEvento(resposta)
		if err != nil {
			log.Printf("[ENTREGA] Falha ao serializar resposta: %v", err)
			return
		}
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, resposta.Type, body)
	})

	log.Println("[ENTREGA] Aguardando pagamentos aprovados. CTRL+C para sair.")
	select {}
}

// ProcessarEntrega valida o pagamento, simula a entrega e assina pedido.enviado.
func ProcessarEntrega(delivery amqp.Delivery) (base.Event, error) {
	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		return base.Event{}, err
	}
	if evento.Type != base.PagamentoAprovado || delivery.RoutingKey != base.PagamentoAprovado || evento.Producer != "pagamento" {
		return base.Event{}, fmt.Errorf("esperado pagamento.aprovado do produtor pagamento")
	}
	valida, err := base.ValidarAssinatura(evento, "entrega/keys/pagamento_public.pem")
	if err != nil {
		return base.Event{}, err
	}
	if !valida {
		return base.Event{}, fmt.Errorf("assinatura inválida")
	}

	var payload base.OrderPayload
	if err := base.DesserializarPayload(evento, &payload); err != nil {
		return base.Event{}, err
	}
	if payload.OrderID <= 0 {
		return base.Event{}, fmt.Errorf("identificador do pedido inválido")
	}

	// A emissão da nota e a preparação da entrega são simuladas no terminal.
	log.Printf("[ENTREGA] Nota do pedido %d emitida (simulação).", payload.OrderID)
	log.Printf("[ENTREGA] Pedido %d preparado para envio (simulação).", payload.OrderID)

	resposta, err := base.CriarEvento(base.PedidoEnviado, "entrega", payload)
	if err != nil {
		return base.Event{}, err
	}
	if err := base.AssinarEvento(&resposta, "entrega/keys/entrega_private.pem"); err != nil {
		return base.Event{}, err
	}
	return resposta, nil
}
