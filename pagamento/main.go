package main

import (
	"fmt"
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

// ProcessarPagamento valida o evento do Estoque e assina o resultado simulado.
// O resultado do sorteio é recebido do main para permitir testar os dois casos.
func ProcessarPagamento(delivery amqp.Delivery, aprovado bool) (base.Event, error) {
	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		return base.Event{}, err
	}
	if evento.Type != base.EstoqueOK || delivery.RoutingKey != base.EstoqueOK || evento.Producer != "estoque" {
		return base.Event{}, fmt.Errorf("esperado pedido.estoque_ok do produtor estoque")
	}
	valida, err := base.ValidarAssinatura(evento, "pagamento/keys/estoque_public.pem")
	if err != nil {
		return base.Event{}, err
	}
	if !valida {
		return base.Event{}, fmt.Errorf("assinatura inválida")
	}

	var payload base.OrderPayload
	if err := base.DesserializarPayload(evento, &payload); err != nil {
		return base.Event{}, err
	}
	if payload.OrderID <= 0 {
		return base.Event{}, fmt.Errorf("identificador do pedido inválido")
	}

	tipo := base.PagamentoRecusado
	if aprovado {
		tipo = base.PagamentoAprovado
	}
	resposta, err := base.CriarEvento(tipo, "pagamento", payload)
	if err != nil {
		return base.Event{}, err
	}
	if err := base.AssinarEvento(&resposta, "pagamento/keys/pagamento_private.pem"); err != nil {
		return base.Event{}, err
	}
	return resposta, nil
}
