package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	rabbitmq.DeclararExchangeEcommerce(ch)
	ConfigurarFilaPrincipal(ch)
	rabbitmq.IniciarConsumo(ch, FilaPrincipal, func(delivery amqp.Delivery) {
		TratarEvento(ch, delivery)
	})

	ExecutarMenu(ch)
}
