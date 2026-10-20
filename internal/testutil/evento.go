// Package testutil cria chaves temporárias para testar os consumidores sem usar
// nem modificar as chaves da aplicação.
package testutil

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
)

func EventoAssinado(t *testing.T, consumidor, produtor, tipo string) (base.Event, base.OrderPayload) {
	t.Helper()
	t.Chdir(t.TempDir())
	privada, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := x509.MarshalPKCS8PrivateKey(privada)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := x509.MarshalPKIXPublicKey(&privada.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(consumidor, "keys")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("private.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, produtor+"_public.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub}), 0600); err != nil {
		t.Fatal(err)
	}
	pedido := base.OrderPayload{OrderID: 1, Items: []base.OrderItem{{ProductID: 1, Quantity: 2}}}
	evento, err := base.CriarEvento(tipo, produtor, pedido)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.AssinarEvento(&evento, "private.pem"); err != nil {
		t.Fatal(err)
	}
	return evento, pedido
}

func Delivery(t *testing.T, evento base.Event) amqp.Delivery {
	t.Helper()
	body, err := base.SerializarEvento(evento)
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Body: body, RoutingKey: evento.Type}
}
