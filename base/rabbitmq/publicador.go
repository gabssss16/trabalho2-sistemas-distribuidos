package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublicarEvento(ch *amqp.Channel, exchange, routingKey string, body []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	FailOnError(err, "Falha em publicar a mensagem")
	log.Printf("[x] Evento publicado com sucesso | Routing Key: %s", routingKey)
}
