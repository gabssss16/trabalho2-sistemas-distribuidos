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

	// Garante que a exchange "Promoções" existe antes de tentar usá-la
	rabbitmq.DeclararExchangePromocoes(ch)

	// C1 cria a própria fila
	fila := rabbitmq.DeclararFila(ch, "fila_c1")

	// C1 se inscreve apenas nas categorias A e B
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.A", rabbitmq.ExchangePromocoes)
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.B", rabbitmq.ExchangePromocoes)

	// Regra do que fazer quando a mensagem chegar
	handler := func(d amqp.Delivery) {
		log.Printf("[C1] Promoção recebida na categoria %s: %s", d.RoutingKey, string(d.Body))
	}

	log.Println("[*] Consumidor C1 aguardando promoções A e B... CTRL+C para sair")
	
	// Usa a goroutine pronta da sua dupla para ficar escutando
	rabbitmq.IniciarConsumo(ch, fila.Name, handler)

	// Trava o programa para ele não fechar imediatamente
	var forever chan struct{}
	<-forever
}