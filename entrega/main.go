package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func main() {
	servico := &servicoEntrega{enviados: make(map[int]bool)}
	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	rabbitmq.DeclararTopologiaEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila.entrega")
	rabbitmq.VincularFila(ch, fila.Name, base.PagamentoAprovado, rabbitmq.ExchangeEcommerce)
	servico.publicar = rabbitmq.NovoPublicadorAssinado(conn, "entrega", "entrega/keys/entrega_private.pem")
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		pedido, err := lerPagamentoAprovado(d)
		if err != nil {
			return err
		}
		return servico.enviar(pedido)
	})
	log.Println("[ENTREGA] Aguardando pagamentos aprovados. CTRL+C para sair.")
	select {}
}

func lerPagamentoAprovado(d amqp.Delivery) (base.OrderPayload, error) {
	var pedido base.OrderPayload
	evento, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return pedido, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if evento.Type != base.PagamentoAprovado || d.RoutingKey != base.PagamentoAprovado || evento.Producer != "pagamento" {
		return pedido, fmt.Errorf("%w: esperado pagamento.aprovado do pagamento", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(evento, "entrega/keys/pagamento_public.pem")
	if err != nil || !ok {
		return pedido, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	if err := validarPedido(pedido); err != nil {
		return pedido, err
	}
	return pedido, nil
}
