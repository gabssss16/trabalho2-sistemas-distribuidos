package rabbitmq

import (
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func Conectar() *amqp.Connection {
	endereco := os.Getenv("RABBITMQ_URL")
	if endereco == "" {
		endereco = "amqp://guest:guest@127.0.0.1:5672/"
	}
	conn, err := amqp.Dial(endereco)
	FailOnError(err, "Falha ao conectar ao RabbitMQ")
	return conn
}

func AbrirCanal(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	FailOnError(err, "Falha ao abrir o canal")
	return ch
}
