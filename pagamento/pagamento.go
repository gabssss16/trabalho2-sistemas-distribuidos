package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"trabalho2-sistemas-distribuidos/base"
)

// ProcessarPagamento valida o evento do Estoque e assina o resultado simulado.
// O resultado do sorteio é recebido do main para permitir testar os dois casos.
func ProcessarPagamento(delivery amqp.Delivery, aprovado bool) (base.Event, error) {
	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		return base.Event{}, err
	}
	if evento.Type != base.EstoqueOK || delivery.RoutingKey != base.EstoqueOK || evento.Producer != "estoque" {
		return base.Event{}, fmt.Errorf("esperado pedido.estoque_ok do produtor estoque")
	}
	valida, err := base.ValidarAssinatura(evento, "pagamento/keys/estoque_public.pem")
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

	tipo := base.PagamentoRecusado
	if aprovado {
		tipo = base.PagamentoAprovado
	}
	resposta, err := base.CriarEvento(tipo, "pagamento", payload)
	if err != nil {
		return base.Event{}, err
	}
	if err := base.AssinarEvento(&resposta, "pagamento/keys/pagamento_private.pem"); err != nil {
		return base.Event{}, err
	}
	return resposta, nil
}
