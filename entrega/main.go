package main

import (
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

	rabbitmq.IniciarConsumo(ch, fila.Name, func(delivery amqp.Delivery) {
		resposta, err := ProcessarEntrega(delivery)
		if err != nil {
			log.Printf("[ENTREGA] Evento não processado: %v", err)
			return
		}
		body, err := base.SerializarEvento(resposta)
		if err != nil {
			log.Printf("[ENTREGA] Falha ao serializar resposta: %v", err)
			return
		}
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, resposta.Type, body)
	})

	log.Println("[ENTREGA] Aguardando pagamentos aprovados. CTRL+C para sair.")
	select {}
}
