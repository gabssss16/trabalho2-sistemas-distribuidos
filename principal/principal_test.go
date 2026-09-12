package main

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
)

// As chaves ficam apenas no diretório temporário do teste.
func prepararChaves(t *testing.T) string {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("principal/keys", 0700); err != nil {
		t.Fatal(err)
	}
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
	caminho := filepath.Join("principal", "keys", "teste_private.pem")
	if err := os.WriteFile(caminho, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privada}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, produtor := range []string{"estoque", "pagamento", "entrega"} {
		if err := os.WriteFile("principal/keys/"+produtor+"_public.pem", pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publica}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return caminho
}

func TestTratarEvento(t *testing.T) {
	privada := prepararChaves(t)
	casos := []struct {
		nome    string
		tipo    string
		status  string
		alterar func(*base.Event)
		espera  string
	}{
		{"estoque disponível", base.EstoqueOK, base.PedidoCriado, nil, base.EstoqueOK},
		{"pagamento aprovado", base.PagamentoAprovado, base.EstoqueOK, nil, base.PagamentoAprovado},
		{"pedido enviado", base.PedidoEnviado, base.PagamentoAprovado, nil, base.PedidoEnviado},
		{"sem assinatura", base.EstoqueOK, base.PedidoCriado, func(e *base.Event) { e.Signature = "" }, base.PedidoCriado},
		{"payload alterado", base.EstoqueOK, base.PedidoCriado, func(e *base.Event) { e.Payload = []byte(`{"order_id":1,"items":[]}`) }, base.PedidoCriado},
		{"produtor incorreto", base.EstoqueOK, base.PedidoCriado, func(e *base.Event) { e.Producer = "entrega" }, base.PedidoCriado},
		{"tipo alterado", base.EstoqueOK, base.PedidoCriado, func(e *base.Event) { e.Type = base.PedidoEnviado }, base.PedidoCriado},
		{"excluído não volta a ativo", base.EstoqueOK, base.PedidoExcluido, nil, base.PedidoExcluido},
		{"enviado não retrocede", base.PagamentoAprovado, base.PedidoEnviado, nil, base.PedidoEnviado},
	}
	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			pedidos = map[int]Pedido{1: {Status: caso.status}}
			evento, err := base.CriarEvento(caso.tipo, produtores[caso.tipo], base.OrderPayload{OrderID: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := base.AssinarEvento(&evento, privada); err != nil {
				t.Fatal(err)
			}
			if caso.alterar != nil {
				caso.alterar(&evento)
			}
			body, err := base.SerializarEvento(evento)
			if err != nil {
				t.Fatal(err)
			}
			TratarEvento(nil, amqp.Delivery{RoutingKey: caso.tipo, Body: body})
			if pedidos[1].Status != caso.espera {
				t.Fatalf("status = %s; esperado %s", pedidos[1].Status, caso.espera)
			}
		})
	}
}

func TestCriarPedidoSemChaveNaoRegistraPedido(t *testing.T) {
	t.Chdir(t.TempDir())
	pedidos = make(map[int]Pedido)
	proximoID = 1
	entrada := bufio.NewScanner(strings.NewReader("1\n1\n2\n"))
	if err := CriarPedido(nil, entrada); err == nil {
		t.Fatal("deveria exigir a chave privada para publicar")
	}
	if len(pedidos) != 0 || proximoID != 1 {
		t.Fatal("um pedido não publicado não deve ser registrado")
	}
}
