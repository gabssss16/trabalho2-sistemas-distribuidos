package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
)

func prepararChaves(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("entrega/keys", 0700); err != nil {
		t.Fatal(err)
	}
	for _, produtor := range []string{"pagamento", "entrega"} {
		chave, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		privada, err := x509.MarshalPKCS8PrivateKey(chave)
		if err != nil {
			t.Fatal(err)
		}
		publica, err := x509.MarshalPKIXPublicKey(&chave.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		for sufixo, bloco := range map[string]*pem.Block{
			"_private.pem": {Type: "PRIVATE KEY", Bytes: privada},
			"_public.pem":  {Type: "PUBLIC KEY", Bytes: publica},
		} {
			caminho := filepath.Join("entrega", "keys", produtor+sufixo)
			if err := os.WriteFile(caminho, pem.EncodeToMemory(bloco), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func entregaAssinada(t *testing.T, payload any) amqp.Delivery {
	t.Helper()
	evento, err := base.CriarEvento(base.PagamentoAprovado, "pagamento", payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.AssinarEvento(&evento, "entrega/keys/pagamento_private.pem"); err != nil {
		t.Fatal(err)
	}
	body, err := base.SerializarEvento(evento)
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{RoutingKey: base.PagamentoAprovado, Body: body}
}

func TestProcessamento(t *testing.T) {
	prepararChaves(t)
	payload := base.OrderPayload{OrderID: 7, Items: []base.OrderItem{{ProductID: 1, Quantity: 2}}}
	delivery := entregaAssinada(t, payload)
	resposta, err := ProcessarEntrega(delivery)
	if err != nil {
		t.Fatal(err)
	}
	conferirResposta(t, resposta, base.PedidoEnviado, payload)
}

func conferirResposta(t *testing.T, resposta base.Event, tipo string, payload base.OrderPayload) {
	t.Helper()
	if resposta.Type != tipo || resposta.Producer != "entrega" {
		t.Fatalf("resposta inesperada: %+v", resposta)
	}
	// Verifica a assinatura depois de passar pelo mesmo JSON usado no RabbitMQ.
	body, err := base.SerializarEvento(resposta)
	if err != nil {
		t.Fatal(err)
	}
	resposta, err = base.DesserializarEvento(body)
	if err != nil {
		t.Fatal(err)
	}
	valida, err := base.ValidarAssinatura(resposta, "entrega/keys/entrega_public.pem")
	if err != nil || !valida {
		t.Fatalf("assinatura da resposta inválida: %v", err)
	}
	var recebido base.OrderPayload
	if err := base.DesserializarPayload(resposta, &recebido); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recebido, payload) {
		t.Fatalf("pedido alterado: %+v", recebido)
	}
}

func TestDescartaEventosInvalidos(t *testing.T) {
	prepararChaves(t)
	for _, caso := range []string{"assinatura ausente", "payload adulterado", "produtor incorreto", "routing key incorreta", "pedido sem ID", "payload incompatível", "assinatura de outro produtor"} {
		t.Run(caso, func(t *testing.T) {
			var payload any = base.OrderPayload{OrderID: 7}
			if caso == "pedido sem ID" {
				payload = base.OrderPayload{}
			}
			if caso == "payload incompatível" {
				payload = "não é um pedido"
			}
			delivery := entregaAssinada(t, payload)
			evento, err := base.DesserializarEvento(delivery.Body)
			if err != nil {
				t.Fatal(err)
			}
			switch caso {
			case "assinatura ausente":
				evento.Signature = ""
			case "payload adulterado":
				evento.Payload = []byte(`{"order_id":8}`)
			case "produtor incorreto":
				evento.Producer = "outro"
				if err := base.AssinarEvento(&evento, "entrega/keys/pagamento_private.pem"); err != nil {
					t.Fatal(err)
				}
			case "routing key incorreta":
				delivery.RoutingKey = base.PedidoCriado
			case "assinatura de outro produtor":
				if err := base.AssinarEvento(&evento, "entrega/keys/entrega_private.pem"); err != nil {
					t.Fatal(err)
				}
			}
			delivery.Body, err = base.SerializarEvento(evento)
			if err != nil {
				t.Fatal(err)
			}
			resposta, err := ProcessarEntrega(delivery)
			if err == nil || resposta.Type != "" {
				t.Fatalf("evento inválido gerou resposta: %+v, %v", resposta, err)
			}
		})
	}
}
