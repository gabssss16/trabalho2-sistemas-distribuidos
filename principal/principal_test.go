package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
	"trabalho2-sistemas-distribuidos/internal/testutil"
)

func novoPrincipal(t *testing.T) *servicoPrincipal {
	t.Helper()
	s, err := abrirPrincipal(filepath.Join(t.TempDir(), "principal.json"))
	if err != nil {
		t.Fatal(err)
	}
	s.publicar = func(string, any) error { return nil }
	return s
}
func requisicao(s *servicoPrincipal, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.rotas().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}
func criar(t *testing.T, s *servicoPrincipal) PedidoResponse {
	t.Helper()
	w := requisicao(s, "POST", "/pedidos", `{"itens":[{"product_id":1,"quantity":1}]}`)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	var p PedidoResponse
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestCORSRotasEMetodos(t *testing.T) {
	s := novoPrincipal(t)
	for _, path := range []string{"/pedidos", "/interesse", "/pedidos/1/cancelar"} {
		r := httptest.NewRequest("OPTIONS", path, nil)
		r.Header.Set("Origin", "http://localhost:5500")
		r.Header.Set("Access-Control-Request-Method", "POST")
		r.Header.Set("Access-Control-Request-Headers", "content-type")
		w := httptest.NewRecorder()
		s.rotas().ServeHTTP(w, r)
		if w.Code != 204 || w.Header().Get("Access-Control-Allow-Headers") == "" {
			t.Fatal(path, w.Code)
		}
	}
	if w := requisicao(s, "GET", "/interesse", ""); w.Code != 405 {
		t.Fatal(w.Code)
	}
}
func TestPedidoHATEOASReinicioEOutbox(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	if w := requisicao(s, "GET", p.Links["self"], ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	s.publicar = func(string, any) error { return fmt.Errorf("broker offline") }
	if s.retomarPendentes() == nil {
		t.Fatal("falha ignorada")
	}
	novo, err := abrirPrincipal(s.caminho)
	if err != nil {
		t.Fatal(err)
	}
	q := criar(t, novo)
	if q.OrderID <= p.OrderID || len(novo.estado.Pendentes) != 2 {
		t.Fatal("ID reutilizado ou pendencia perdida")
	}
	n := 0
	novo.publicar = func(kind string, data any) error {
		n++
		if kind != base.PedidoCriado {
			t.Error(kind)
		}
		return nil
	}
	if err := novo.retomarPendentes(); err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(novo.estado.Pendentes) != 0 {
		t.Fatal("nao retomou")
	}
}
func TestPersistenciaFalhaNaoAceitaPedido(t *testing.T) {
	s := novoPrincipal(t)
	id := s.estado.ProximoID
	s.caminho = t.TempDir()
	w := requisicao(s, "POST", "/pedidos", `{"itens":[{"product_id":1,"quantity":1}]}`)
	if w.Code != 503 || len(s.estado.Pedidos) != 0 || s.estado.ProximoID != id {
		t.Fatal("estado alterado sem salvar")
	}
}
func TestEntradasInvalidas(t *testing.T) {
	s := novoPrincipal(t)
	for _, body := range []string{`{`, `{} {}`, `{"itens":[]}`, `{"itens":[{"product_id":1,"quantity":0}]}`, `{"itens":[{"product_id":0,"quantity":1}]}`} {
		if w := requisicao(s, "POST", "/pedidos", body); w.Code != 400 {
			t.Fatal(body, w.Code)
		}
	}
	for _, body := range []string{`{"email":"@","categorias":["A"]}`, `{"email":"ana@example.com","categorias":["Z"]}`, `{"email":"ana@example.com","categorias":[]}`} {
		if w := requisicao(s, "POST", "/interesse", body); w.Code != 400 {
			t.Fatal(body, w.Code)
		}
	}
}
func TestInteressesVersionadosPersistidosECancelamentoCompleto(t *testing.T) {
	s := novoPrincipal(t)
	req := func(method, body string) {
		t.Helper()
		if w := requisicao(s, method, "/interesse", body); w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	req("POST", `{"email":" ANA@example.com ","categorias":["A","B"]}`)
	v := s.estado.Interesses["ana@example.com"].Versao
	req("DELETE", `{"email":"ana@example.com","categorias":["A"]}`)
	if s.estado.Interesses["ana@example.com"].Versao <= v {
		t.Fatal("versao repetida")
	}
	var p base.InteressePromocaoPayload
	json.Unmarshal(s.estado.Pendentes[1].Payload, &p)
	if p.Acao != "cadastrar" || !reflect.DeepEqual(p.Categorias, []string{"B"}) {
		t.Fatal(p)
	}
	novo, err := abrirPrincipal(s.caminho)
	if err != nil {
		t.Fatal(err)
	}
	s = novo
	req("DELETE", `{"email":"ana@example.com"}`)
	p = base.InteressePromocaoPayload{}
	json.Unmarshal(s.estado.Pendentes[2].Payload, &p)
	if p.Acao != "cancelar" || len(p.Categorias) != 0 {
		t.Fatal(p)
	}
}
func TestConcorrenciaIDsEPublicacaoNaoBloqueiaAPI(t *testing.T) {
	s := novoPrincipal(t)
	var wg sync.WaitGroup
	ids := make(chan int, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := requisicao(s, "POST", "/pedidos", `{"itens":[{"product_id":1,"quantity":1}]}`)
			var p PedidoResponse
			json.Unmarshal(w.Body.Bytes(), &p)
			if w.Code != 201 {
				t.Error(w.Code)
			}
			ids <- p.OrderID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[int]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatal("ID repetido")
		}
		seen[id] = true
	}
	iniciou := make(chan struct{})
	liberar := make(chan struct{})
	s.publicar = func(string, any) error { close(iniciou); <-liberar; return fmt.Errorf("simulado") }
	done := make(chan struct{})
	go func() { s.retomarPendentes(); close(done) }()
	<-iniciou
	if !s.mu.TryLock() {
		close(liberar)
		<-done
		t.Fatal("publicacao reteve mutex")
	}
	s.mu.Unlock()
	close(liberar)
	<-done
}
func TestEventosAssinadosRecusaPersisteDevolucao(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	e, _ := testutil.EventoAssinado(t, "principal", "pagamento", base.PagamentoRecusado)
	e.Payload, _ = json.Marshal(base.OrderPayload{OrderID: p.OrderID})
	base.AssinarEvento(&e, "private.pem")
	if err := s.tratarEvento(testutil.Delivery(t, e)); err != nil {
		t.Fatal(err)
	}
	if err := s.tratarEvento(testutil.Delivery(t, e)); err != nil {
		t.Fatal(err)
	}
	if len(s.estado.Pendentes) != 2 || s.estado.Pendentes[1].Tipo != base.PedidoExcluido {
		t.Fatal("compensacao ausente/duplicada")
	}
	e.Signature = "invalida"
	if err := s.tratarEvento(testutil.Delivery(t, e)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("assinatura invalida aceita")
	}
}
func TestEstadoCausalNaoRegride(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	if err := s.aplicarEvento(base.PedidoEnviado, p.OrderID); err != nil {
		t.Fatal(err)
	}
	s.aplicarEvento(base.PagamentoAprovado, p.OrderID)
	s.aplicarEvento(base.EstoqueOK, p.OrderID)
	want := []string{base.PedidoCriado, base.EstoqueOK, base.PagamentoAprovado, base.PedidoEnviado}
	if !reflect.DeepEqual(s.estado.Pedidos[p.OrderID].Historico, want) {
		t.Fatal(s.estado.Pedidos[p.OrderID])
	}
}
func TestSSEDuasAbasReconexaoEClienteLento(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	srv := httptest.NewServer(s.rotas())
	defer srv.Close()
	abrir := func(last string) (*http.Response, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+p.Links["acompanhar_sse"], nil)
		req.Header.Set("Last-Event-ID", last)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			cancel()
			t.Fatal(resp.StatusCode)
		}
		return resp, cancel
	}
	a, ca := abrir("")
	defer ca()
	defer a.Body.Close()
	b, cb := abrir("")
	defer cb()
	defer b.Body.Close()
	ca()
	a.Body.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := len(s.clientes[p.OrderID])
		s.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s.mu.Lock()
	n := len(s.clientes[p.OrderID])
	s.mu.Unlock()
	if n != 1 {
		t.Fatal("fechar aba afetou outra")
	}
	if err := s.aplicarEvento(base.EstoqueOK, p.OrderID); err != nil {
		t.Fatal(err)
	}
	if err := s.aplicarEvento(base.PagamentoAprovado, p.OrderID); err != nil {
		t.Fatal(err)
	}
	if err := s.aplicarEvento(base.PedidoEnviado, p.OrderID); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(b.Body)
	var eventos []string
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			eventos = append(eventos, strings.TrimPrefix(scanner.Text(), "data: "))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(eventos) != 4 {
		t.Fatal(eventos)
	}
	c, cc := abrir("2")
	defer cc()
	defer c.Body.Close()
	scanner = bufio.NewScanner(c.Body)
	eventos = nil
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			eventos = append(eventos, scanner.Text())
		}
	}
	if !reflect.DeepEqual(eventos, []string{"data: pagamento.aprovado", "data: pedido.enviado"}) {
		t.Fatal(eventos)
	}
}
func TestSSEIgnoraSinalRepetidoSemPerderHistorico(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	lento := make(chan struct{}, 1)
	s.clientes[p.OrderID] = map[chan struct{}]struct{}{lento: {}}
	for _, st := range []string{base.EstoqueOK, base.PagamentoAprovado, base.PedidoEnviado} {
		if err := s.aplicarEvento(st, p.OrderID); err != nil {
			t.Fatal(err)
		}
	}
	if len(lento) != 1 || len(s.estado.Pedidos[p.OrderID].Historico) != 4 {
		t.Fatal("historico perdido")
	}
}
func TestProxyCheckoutEsperaETroca404Por200(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			responderJSON(w, 404, map[string]string{"error": "aguarde"})
			return
		}
		responderJSON(w, 200, map[string]string{"checkout_url": "http://mock/checkout/1"})
	}))
	defer backend.Close()
	s.pagamentoURL = backend.URL
	for _, want := range []int{404, 200} {
		if w := requisicao(s, "GET", p.Links["pagamento"], ""); w.Code != want {
			t.Fatal(w.Code)
		}
	}
}
func TestCancelamentoAguardaDecisaoDoPagamento(t *testing.T) {
	s := novoPrincipal(t)
	p := criar(t, s)
	w := requisicao(s, "POST", p.Links["cancelar"], "")
	if w.Code != 202 || s.estado.Pendentes[1].Tipo != base.PedidoCancelamentoSolicitado {
		t.Fatal("cancelamento nao coordenado")
	}
	if s.estado.Pendentes[1].Tipo == base.PedidoExcluido {
		t.Fatal("liberou antes de arbitrar pagamento")
	}
	s.aplicarEvento(base.PagamentoAprovado, p.OrderID)
	if w := requisicao(s, "POST", p.Links["cancelar"], ""); w.Code != 409 {
		t.Fatal(w.Code)
	}
}
func TestArquivoCorrompidoNaoReiniciaIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "principal.json")
	for _, b := range []string{`{`, `{}`, `null`} {
		os.WriteFile(path, []byte(b), 0600)
		if _, err := abrirPrincipal(path); err == nil {
			t.Fatal("arquivo invalido aceito")
		}
	}
}
func TestFrontendServidoNaMesmaOrigem(t *testing.T) {
	s := novoPrincipal(t)
	for _, path := range []string{"/", "/app.js"} {
		w := requisicao(s, "GET", path, "")
		if w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
}
