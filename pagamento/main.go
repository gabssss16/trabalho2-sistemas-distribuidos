package main

import (
	"log"
	"math/rand/v2"

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
	fila := rabbitmq.DeclararFila(ch, "fila.pagamento")
	rabbitmq.VincularFila(ch, fila.Name, base.EstoqueOK, rabbitmq.ExchangeEcommerce)

	rabbitmq.IniciarConsumo(ch, fila.Name, func(delivery amqp.Delivery) {
		// Simula o pagamento com chances iguais de aprovação e recusa.
		resposta, err := ProcessarPagamento(delivery, rand.IntN(2) == 0)
		if err != nil {
			log.Printf("[PAGAMENTO] Evento não processado: %v", err)
			return
		}
		body, err := base.SerializarEvento(resposta)
		if err != nil {
			log.Printf("[PAGAMENTO] Falha ao serializar resposta: %v", err)
			return
		}
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, resposta.Type, body)
	})

	log.Println("[PAGAMENTO] Aguardando pedidos. CTRL+C para sair.")
	select {}
}
