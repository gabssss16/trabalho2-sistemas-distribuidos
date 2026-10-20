package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
	"trabalho2-sistemas-distribuidos/internal/testutil"
)

func pedidoTeste() base.OrderPayload {
	return base.OrderPayload{OrderID: 7, Items: []base.OrderItem{{ProductID: 1, Quantity: 2}}}
}

func novaEntrega(t *testing.T) *servicoEntrega {
	t.Helper()
	s, err := abrirEntrega(filepath.Join(t.TempDir(), "entregas.json"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEntregaPersistidaIdempotenteEConcorrente(t *testing.T) {
	s := novaEntrega(t)
	publicados := 0
	s.publicar = func(tipo string, pedido base.OrderPayload) error {
		publicados++
		if tipo != base.PedidoEnviado || !reflect.DeepEqual(pedido, pedidoTeste()) {
			t.Errorf("evento incorreto: %s %+v", tipo, pedido)
		}
		return nil
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.enviar(pedidoTeste()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	original := s.entregas[7]
	if original.NotaFiscal == "" || original.Rastreio == "" || original.EnviadoEm.IsZero() || !original.Publicado {
		t.Fatal("despacho incompleto")
	}
	reiniciado, err := abrirEntrega(s.caminho)
	if err != nil {
		t.Fatal(err)
	}
	reiniciado.publicar = s.publicar
	if err := reiniciado.enviar(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	if publicados != 1 || !reflect.DeepEqual(original, reiniciado.entregas[7]) {
		t.Fatal("duplicou ou alterou entrega")
	}
	outro := pedidoTeste()
	outro.Items[0].Quantity++
	if err := reiniciado.enviar(outro); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("aceitou reutilização de pedido")
	}
}

func TestEntregaRetomaPublicacaoSemNovoDespacho(t *testing.T) {
	s := novaEntrega(t)
	s.publicar = func(string, base.OrderPayload) error { return fmt.Errorf("broker indisponível") }
	if err := s.enviar(pedidoTeste()); err == nil {
		t.Fatal("ignorou falha do broker")
	}
	antes := s.entregas[7]
	if antes.Publicado {
		t.Fatal("confirmou publicação com falha")
	}
	reiniciado, err := abrirEntrega(s.caminho)
	if err != nil {
		t.Fatal(err)
	}
	reiniciado.publicar = func(string, base.OrderPayload) error { return nil }
	if err := reiniciado.enviar(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	depois := reiniciado.entregas[7]
	antes.Publicado = true
	if !reflect.DeepEqual(antes, depois) {
		t.Fatal("repetiu o despacho em vez de retomar publicação")
	}
}

func TestEntregaNaoPublicaSemPersistir(t *testing.T) {
	s := novaEntrega(t)
	s.caminho = filepath.Join(s.caminho, "invalido.json") // pai é um arquivo
	s.publicar = func(string, base.OrderPayload) error { t.Fatal("publicou antes de persistir"); return nil }
	if err := s.enviar(pedidoTeste()); err == nil || len(s.entregas) != 0 {
		t.Fatal("falha de persistência ignorada")
	}
}

func TestEntregaRecusaArquivoInvalido(t *testing.T) {
	for _, conteudo := range []string{`null`, `{`, `{"7":{}}`} {
		path := filepath.Join(t.TempDir(), "entregas.json")
		if err := os.WriteFile(path, []byte(conteudo), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := abrirEntrega(path); err == nil {
			t.Fatal("aceitou arquivo inválido")
		}
	}
}

func TestEntregaValidaPagamentoAssinado(t *testing.T) {
	evento, pedido := testutil.EventoAssinado(t, "entrega", "pagamento", base.PagamentoAprovado)
	recebido, err := lerPagamentoAprovado(testutil.Delivery(t, evento))
	if err != nil || !reflect.DeepEqual(recebido, pedido) {
		t.Fatalf("evento válido: %v", err)
	}
	for _, mudar := range []func(*base.Event){
		func(e *base.Event) { e.Producer = "estoque" },
		func(e *base.Event) { e.Type = base.PagamentoRecusado },
		func(e *base.Event) { e.Signature = "invalida" },
		func(e *base.Event) { e.Payload = []byte(`{"order_id":9}`) },
	} {
		alterado := evento
		mudar(&alterado)
		if _, err := lerPagamentoAprovado(testutil.Delivery(t, alterado)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
			t.Fatal("aceitou evento alterado")
		}
	}
	d := testutil.Delivery(t, evento)
	d.RoutingKey = base.PagamentoRecusado
	if _, err := lerPagamentoAprovado(d); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("aceitou routing key incorreta")
	}
	for _, p := range []base.OrderPayload{
		{OrderID: 1}, {OrderID: 0, Items: pedido.Items},
		{OrderID: 1, Items: []base.OrderItem{{ProductID: 1, Quantity: 0}}},
		{OrderID: 1, Items: []base.OrderItem{{ProductID: 0, Quantity: 1}}},
	} {
		e, err := base.CriarEvento(base.PagamentoAprovado, "pagamento", p)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.AssinarEvento(&e, "private.pem"); err != nil {
			t.Fatal(err)
		}
		if _, err := lerPagamentoAprovado(testutil.Delivery(t, e)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
			t.Fatal("aceitou pedido inválido assinado")
		}
	}
}
