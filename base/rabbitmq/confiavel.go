package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
)

// ErrEventoInvalido identifica mensagens que não devem ser reenfileiradas.
var ErrEventoInvalido = errors.New("evento inválido")

// NovoPublicadorAssinado usa canal exclusivo e aguarda confirmação do broker.
func NovoPublicadorAssinado(conn *amqp.Connection, produtor, chave string) func(string, base.OrderPayload) error {
	ch := AbrirCanal(conn)
	FailOnError(ch.Confirm(false), "Falha ao habilitar confirmações")
	var mu sync.Mutex
	return func(tipo string, pedido base.OrderPayload) error {
		evento, err := base.CriarEvento(tipo, produtor, pedido)
		if err != nil {
			return err
		}
		if err = base.AssinarEvento(&evento, chave); err != nil {
			return err
		}
		body, err := base.SerializarEvento(evento)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		confirmacao, err := ch.PublishWithDeferredConfirmWithContext(ctx, ExchangeEcommerce, tipo, false, false, amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body,
		})
		if err != nil {
			return err
		}
		ok, err := confirmacao.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("broker recusou %s", tipo)
		}
		return nil
	}
}

// ConsumirConfirmado confirma somente após persistir/processar a mensagem.
func ConsumirConfirmado(ch *amqp.Channel, fila string, handler func(amqp.Delivery) error) {
	FailOnError(ch.Qos(1, 0, false), "Falha ao configurar prefetch")
	msgs, err := ch.Consume(fila, "", false, false, false, false, nil)
	FailOnError(err, "Falha ao registrar consumidor")
	go func() {
		for d := range msgs {
			err := handler(d)
			if err == nil {
				err = d.Ack(false)
			} else {
				log.Printf("[%s] %v", fila, err)
				repetir := !errors.Is(err, ErrEventoInvalido)
				if repetir {
					time.Sleep(time.Second)
				}
				err = d.Nack(false, repetir)
			}
			if err != nil {
				log.Printf("[%s] Falha ao confirmar consumo: %v", fila, err)
			}
		}
		log.Fatalf("[%s] Canal de consumo encerrado; reinicie o serviço", fila)
	}()
}
