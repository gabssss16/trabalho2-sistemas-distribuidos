package main

import (
	"errors"
	"testing"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
	"trabalho2-sistemas-distribuidos/internal/testutil"
)

func TestValidaEventoEstoqueAssinado(t *testing.T) {
	evento, pedido := testutil.EventoAssinado(t, "pagamento", "estoque", base.EstoqueOK)
	recebido, err := lerPedidoEstoque(testutil.Delivery(t, evento))
	if err != nil || !pedidosIguais(recebido, pedido) {
		t.Fatalf("evento válido: %v", err)
	}
	d := testutil.Delivery(t, evento)
	d.RoutingKey = base.PedidoCriado
	if _, err := lerPedidoEstoque(d); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("routing key divergente aceita")
	}
	evento.Signature = "invalida"
	if _, err := lerPedidoEstoque(testutil.Delivery(t, evento)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("assinatura inválida aceita")
	}
	evento.Producer = "principal"
	if _, err := lerPedidoEstoque(testutil.Delivery(t, evento)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("produtor inválido aceito")
	}
}
