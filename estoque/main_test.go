package main

import (
	"errors"
	"testing"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
	"trabalho2-sistemas-distribuidos/internal/testutil"
)

func TestValidaOrigemAssinaturaERoutingKey(t *testing.T) {
	evento, pedido := testutil.EventoAssinado(t, "estoque", "principal", base.PedidoCriado)
	recebido, tipo, err := lerPedido(testutil.Delivery(t, evento))
	if err != nil || tipo != base.PedidoCriado || recebido.OrderID != pedido.OrderID {
		t.Fatalf("evento válido: %s %v", tipo, err)
	}
	d := testutil.Delivery(t, evento)
	d.RoutingKey = base.PedidoExcluido
	if _, _, err := lerPedido(d); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("routing key divergente aceita")
	}
	evento.Payload = []byte(`{"order_id":999,"items":[{"product_id":1,"quantity":2}]}`)
	if _, _, err := lerPedido(testutil.Delivery(t, evento)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("payload adulterado aceito")
	}
}
