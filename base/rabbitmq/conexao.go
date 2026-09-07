package rabbitmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func Conectar() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	FailOnError(err, "Falha ao conectar ao RabbitMQ")
	return conn
}

func AbrirCanal(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	FailOnError(err, "Falha ao abrir o canal")
	return ch
}
