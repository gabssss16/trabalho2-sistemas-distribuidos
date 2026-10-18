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

	// Instancia o publicador "Sedex com AR" criado pela sua dupla
	publicar := rabbitmq.NovoPublicadorAssinado(conn, "entrega", "entrega/keys/entrega_private.pem")

	// Usa o consumidor "Sedex com AR" criado pela sua dupla
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(delivery amqp.Delivery) error {
		pedido, err := ProcessarPagamentoAprovado(delivery)
		if err != nil {
			return err // Se der erro, o Confiavel.go devolve a mensagem pra fila
		}

		// Publica o evento de pedido enviado
		return publicar(base.PedidoEnviado, pedido)
	})

	log.Println("[ENTREGA] Aguardando pagamentos aprovados. CTRL+C para sair.")
	select {}
}

// ProcessarPagamentoAprovado valida o evento e simula a nota fiscal.
func ProcessarPagamentoAprovado(delivery amqp.Delivery) (base.OrderPayload, error) {
	var pedido base.OrderPayload

	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		return pedido, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}

	if evento.Type != base.PagamentoAprovado || delivery.RoutingKey != base.PagamentoAprovado || evento.Producer != "pagamento" {
		return pedido, fmt.Errorf("%w: esperado pagamento.aprovado", rabbitmq.ErrEventoInvalido)
	}

	valida, err := base.ValidarAssinatura(evento, "entrega/keys/pagamento_public.pem")
	if err != nil || !valida {
		return pedido, fmt.Errorf("%w: assinatura inválida", rabbitmq.ErrEventoInvalido)
	}

	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}

	if pedido.OrderID <= 0 {
		return pedido, fmt.Errorf("%w: identificador do pedido inválido", rabbitmq.ErrEventoInvalido)
	}

	// === SIMULAÇÃO BÁSICA DE NOTA FISCAL ===
	notaFiscal := fmt.Sprintf("NF-SIM-%d", pedido.OrderID)
	rastreio := fmt.Sprintf("ENVIO-SIM-%d", pedido.OrderID)
	
	log.Printf("[ENTREGA] Pedido %d preparado! Nota: %s | Rastreio: %s", pedido.OrderID, notaFiscal, rastreio)

	return pedido, nil
}