package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	// Garante que a exchange "Promoções" (topic) existe antes de tentar usá-la
	rabbitmq.DeclararExchangePromocoes(ch)

	// C2 cria sua própria fila
	fila := rabbitmq.DeclararFila(ch, "fila_c2")

	// C2 se inscreve em TODAS as categorias usando o curinga asterisco
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.*", rabbitmq.ExchangePromocoes)

	// Regra do que fazer quando a mensagem chegar
	handler := func(d amqp.Delivery) {
		log.Printf("[C2] Promoção recebida na categoria %s: %s", d.RoutingKey, string(d.Body))
	}

	log.Println("[*] Consumidor C2 aguardando TODAS as promoções... CTRL+C para sair")

	// Inicia o consumo assíncrono em segundo plano
	rabbitmq.IniciarConsumo(ch, fila.Name, handler)

	// Trava a execução para o terminal não fechar
	var forever chan struct{}
	<-forever
}