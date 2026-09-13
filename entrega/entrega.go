package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"trabalho2-sistemas-distribuidos/base"
)

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
