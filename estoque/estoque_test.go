package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func novoEstoque(t *testing.T) *estoquePersistente {
	t.Helper()
	s, err := abrirEstoque(filepath.Join(t.TempDir(), "estoque.json"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pedido(id, produto, quantidade int) base.OrderPayload {
	return base.OrderPayload{OrderID: id, Items: []base.OrderItem{{ProductID: produto, Quantity: quantidade}}}
}

func TestReservaPersisteEDevolveUmaVez(t *testing.T) {
	s := novoEstoque(t)
	p := pedido(1, 1, 4)
	for range 2 {
		tipo, err := s.processar(base.PedidoCriado, p)
		if err != nil || tipo != base.EstoqueOK {
			t.Fatalf("reserva: %s %v", tipo, err)
		}
	}
	reiniciado, err := abrirEstoque(s.caminho)
	if err != nil {
		t.Fatal(err)
	}
	if got := reiniciado.estado.Produtos[1].Quantidade; got != 11 {
		t.Fatalf("após reinício: %d", got)
	}
	for range 2 {
		// A devolução independe dos itens enviados pelo cancelamento.
		if _, err := reiniciado.processar(base.PedidoExcluido, base.OrderPayload{OrderID: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := reiniciado.estado.Produtos[1].Quantidade; got != 15 {
		t.Fatalf("devolução: %d", got)
	}
	if tipo, err := reiniciado.processar(base.PedidoCriado, p); err != nil || tipo != "" {
		t.Fatalf("recriou cancelado: %s %v", tipo, err)
	}
}

func TestIndisponivelNaoAlteraEstoque(t *testing.T) {
	for _, itens := range [][]base.OrderItem{
		{{ProductID: 1, Quantity: 10}, {ProductID: 1, Quantity: 6}},
		{{ProductID: 1, Quantity: 1}, {ProductID: 2, Quantity: 11}},
		{{ProductID: 999, Quantity: 1}},
	} {
		s := novoEstoque(t)
		p := base.OrderPayload{OrderID: 1, Items: itens}
		tipo, err := s.processar(base.PedidoCriado, p)
		if err != nil || tipo != base.EstoqueIndisponivel {
			t.Fatalf("%s %v", tipo, err)
		}
		if _, err := s.processar(base.PedidoExcluido, p); err != nil {
			t.Fatal(err)
		}
		if s.estado.Produtos[1].Quantidade != 15 || s.estado.Produtos[2].Quantidade != 10 || len(s.estado.Produtos) != 3 {
			t.Fatal("estoque alterado por pedido recusado")
		}
	}
}

func TestPedidoInvalidoEIdentificadorReutilizado(t *testing.T) {
	s := novoEstoque(t)
	for _, p := range []base.OrderPayload{pedido(0, 1, 1), pedido(1, 1, 0), pedido(1, 1, -1), pedido(1, 0, 1), {OrderID: 1}} {
		if _, err := s.processar(base.PedidoCriado, p); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
			t.Fatalf("pedido inválido aceito: %+v %v", p, err)
		}
	}
	if _, err := s.processar(base.PedidoCriado, pedido(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.processar(base.PedidoCriado, pedido(1, 1, 2)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("aceitou ID reutilizado")
	}
}

func TestCancelamentoAntesDaCriacao(t *testing.T) {
	s := novoEstoque(t)
	if _, err := s.processar(base.PedidoExcluido, base.OrderPayload{OrderID: 1}); err != nil {
		t.Fatal(err)
	}
	if tipo, err := s.processar(base.PedidoCriado, pedido(1, 1, 1)); err != nil || tipo != "" {
		t.Fatalf("%s %v", tipo, err)
	}
	if s.estado.Produtos[1].Quantidade != 15 {
		t.Fatal("pedido cancelado reservou estoque")
	}
}

func TestPersistenciaFalhaSemBaixaEmMemoria(t *testing.T) {
	s := novoEstoque(t)
	s.caminho = t.TempDir() // Rename de arquivo sobre diretório deve falhar.
	if _, err := s.processar(base.PedidoCriado, pedido(1, 1, 1)); err == nil {
		t.Fatal("esperada falha de gravação")
	}
	if s.estado.Produtos[1].Quantidade != 15 || len(s.estado.Pedidos) != 0 {
		t.Fatal("estado alterado sem persistir")
	}
}

func TestArquivoCorrompidoNaoReinicializaEstoque(t *testing.T) {
	caminho := filepath.Join(t.TempDir(), "estado.json")
	if err := os.WriteFile(caminho, []byte("{corrompido"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := abrirEstoque(caminho); err == nil {
		t.Fatal("arquivo corrompido aceito")
	}
}

func TestConcorrenciaEProdutosDisponiveis(t *testing.T) {
	s := novoEstoque(t)
	var wg sync.WaitGroup
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if _, err := s.processar(base.PedidoCriado, pedido(id, 1, 1)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if s.estado.Produtos[1].Quantidade != 0 {
		t.Fatal("quantidade incorreta após concorrência")
	}
	rec := httptest.NewRecorder()
	s.rotas().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/produtos", nil))
	var produtos []produto
	if err := json.Unmarshal(rec.Body.Bytes(), &produtos); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 200 || len(produtos) != 2 || produtos[0].ID != 2 {
		t.Fatalf("resposta: %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.rotas().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/produtos", nil))
	if rec.Code != 405 {
		t.Fatalf("método inválido: %d", rec.Code)
	}
}
