package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// IniciarConsumo registra o consumidor e processa as mensagens em uma goroutine.
// Quem chama deve manter o programa, a conexão e o canal abertos durante o consumo.
func IniciarConsumo(ch *amqp.Channel, nomeFila string, handler func(amqp.Delivery)) {
	msgs, err := ch.Consume(
		nomeFila, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	FailOnError(err, "Falha ao registrar o consumidor")

	go func() {
		for d := range msgs {
			handler(d)
		}
	}()
}
