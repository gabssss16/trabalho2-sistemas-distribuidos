package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
)

const (
	ExchangeEcommerce = "eCommerce"
	ExchangePromocoes = "Promoções"
)

func DeclararExchangeEcommerce(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		ExchangeEcommerce, // name
		"direct",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)

	FailOnError(err, "Falha ao declarar a exchange eCommerce")
}

func DeclararExchangePromocoes(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		ExchangePromocoes, // name
		"topic",           // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)

	FailOnError(err, "Falha ao declarar a exchange Promoções")
}

func DeclararFila(ch *amqp.Channel, nome string) amqp.Queue {
	fila, err := ch.QueueDeclare(
		nome,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	FailOnError(err, "Falha ao declarar a fila "+nome)

	return fila
}

func VincularFila(ch *amqp.Channel, nomeFila, routingKey, exchange string) {
	err := ch.QueueBind(
		nomeFila,   // queue name
		routingKey, // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // arguments
	)

	FailOnError(err, "Falha ao vincular a fila "+nomeFila+" à exchange "+exchange)
}

// Declara todos os destinos antes da primeira publicacao. Assim, um consumidor
// temporariamente offline nao faz perder sua copia do evento por falta de fila.
func DeclararTopologiaEcommerce(ch *amqp.Channel) {
	DeclararExchangeEcommerce(ch)
	destinos := map[string][]string{
		"fila.principal":            {base.EstoqueOK, base.EstoqueIndisponivel, base.PagamentoAprovado, base.PagamentoRecusado, base.PedidoEnviado},
		"fila_estoque":              {base.PedidoCriado, base.PedidoExcluido},
		"fila.pagamento":            {base.EstoqueOK, base.PedidoCancelamentoSolicitado},
		"fila.entrega":              {base.PagamentoAprovado},
		"fila.promocoes.interesses": {base.InteressePromocao},
	}
	for fila, tipos := range destinos {
		DeclararFila(ch, fila)
		for _, tipo := range tipos {
			VincularFila(ch, fila, tipo, ExchangeEcommerce)
		}
	}
}
