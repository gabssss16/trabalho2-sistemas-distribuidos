package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

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
