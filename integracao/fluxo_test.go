package integracao

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

// Execucao opt-in: use um RabbitMQ exclusivo para estes testes.
func TestFluxoDistribuido(t *testing.T) {
	broker := os.Getenv("TEST_RABBITMQ_URL")
	if broker == "" {
		t.Skip("configure TEST_RABBITMQ_URL com um broker isolado")
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	cliente := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	porta := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		return addr
	}
	gateway, stock, pay, mock := porta(), porta(), porta(), porta()
	var envios atomic.Int32
	var indisponivel atomic.Bool
	indisponivel.Store(true)
	email := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer teste-local" || !strings.HasPrefix(r.Header.Get("Idempotency-Key"), "promocao/") {
			t.Error("contrato e-mail")
		}
		if indisponivel.Load() {
			w.WriteHeader(503)
			return
		}
		envios.Add(1)
		fmt.Fprint(w, `{"id":"email-simulado"}`)
	}))
	defer email.Close()
	env := append(os.Environ(), "RABBITMQ_URL="+broker, "PRINCIPAL_ADDR="+gateway, "ESTOQUE_ADDR="+stock, "PAGAMENTO_ADDR="+pay, "MOCK_ADDR="+mock, "ESTOQUE_URL=http://"+stock, "PAGAMENTO_URL=http://"+pay, "MOCK_PAGAMENTO_URL=http://"+mock+"/cobrancas", "PAGAMENTO_WEBHOOK_URL=http://"+pay+"/webhook", "MOCK_PUBLIC_URL=http://"+mock, "RESEND_API_KEY=teste-local", "PROMOCOES_EMAIL_FROM=loja@example.invalid", "PROMOCOES_EMAIL_API_URL="+email.URL, "PROMOCOES_INTERVALO=100ms", "PRINCIPAL_DATA_FILE="+filepath.Join(tmp, "principal.json"), "ESTOQUE_DATA_FILE="+filepath.Join(tmp, "estoque.json"), "PAGAMENTO_DATA_FILE="+filepath.Join(tmp, "pagamento.json"), "MOCK_DATA_FILE="+filepath.Join(tmp, "mock.json"), "ENTREGA_DATA_FILE="+filepath.Join(tmp, "entrega.json"), "PROMOCOES_DATA_FILE="+filepath.Join(tmp, "promocoes.json"))
	type processo struct {
		cmd *exec.Cmd
		log *os.File
	}
	processos := map[string]processo{}
	parar := func(nome string) {
		if p, ok := processos[nome]; ok {
			p.cmd.Process.Kill()
			p.cmd.Wait()
			p.log.Close()
			delete(processos, nome)
		}
	}
	t.Cleanup(func() {
		for name := range processos {
			parar(name)
		}
		if t.Failed() {
			for _, nome := range []string{"principal", "estoque", "pagamento", "mock-pagamento", "entrega", "promocoes"} {
				b, _ := os.ReadFile(filepath.Join(tmp, nome+".log"))
				t.Logf("%s: %s", nome, b)
			}
		}
	})
	iniciar := func(nome string) {
		t.Helper()
		f, err := os.OpenFile(filepath.Join(tmp, nome+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(filepath.Join(tmp, nome))
		cmd.Dir = root
		cmd.Env = env
		cmd.Stdout = f
		cmd.Stderr = f
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		processos[nome] = processo{cmd, f}
	}
	for _, nome := range []string{"principal", "estoque", "pagamento", "mock-pagamento", "entrega", "promocoes"} {
		cmd := exec.Command("go", "build", "-race", "-o", filepath.Join(tmp, nome), "./"+nome)
		cmd.Dir = root
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v %s", nome, err, b)
		}
	}
	for _, nome := range []string{"estoque", "pagamento", "entrega", "mock-pagamento", "promocoes", "principal"} {
		iniciar(nome)
	}
	aguardar := func(descricao string, f func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if f() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("timeout: %s", descricao)
	}
	chamar := func(method, path, body string) (int, []byte) {
		req, err := http.NewRequest(method, path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(body, "status=") {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := cliente.Do(req)
		if err != nil {
			return 0, nil
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	api := "http://" + gateway
	aguardar("servidor principal", func() bool { st, _ := chamar("GET", api+"/produtos", ""); return st == 200 })
	conn, err := amqp.Dial(broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	aguardar("consumidores AMQP", func() bool {
		for _, q := range []string{"fila.principal", "fila_estoque", "fila.pagamento", "fila.entrega", "fila.promocoes.interesses"} {
			v, err := ch.QueueInspect(q)
			if err != nil || v.Consumers != 1 {
				return false
			}
		}
		return true
	})
	// Confirmacao do broker nao deve esconder ausencia de destino.
	publicar := rabbitmq.NovoPublicadorDeEventos(conn, "principal", filepath.Join(root, "principal/keys/principal_private.pem"))
	if err := publicar("teste.sem.destino", base.OrderPayload{OrderID: 1}); err == nil {
		t.Fatal("publicacao sem fila retornou sucesso")
	}
	criar := func(qtd int) int {
		t.Helper()
		status, b := chamar("POST", api+"/pedidos", fmt.Sprintf(`{"itens":[{"product_id":1,"quantity":%d}]}`, qtd))
		var p struct {
			ID int `json:"order_id"`
		}
		json.Unmarshal(b, &p)
		if status != 201 || p.ID <= 0 {
			t.Fatalf("criar: %d %s", status, b)
		}
		return p.ID
	}
	estado := func(id int) string {
		_, b := chamar("GET", fmt.Sprintf("%s/pedidos/%d", api, id), "")
		var p struct {
			Status string `json:"status"`
		}
		json.Unmarshal(b, &p)
		return p.Status
	}
	checkout := func(id int) string {
		t.Helper()
		var u string
		aguardar("checkout", func() bool {
			st, b := chamar("GET", fmt.Sprintf("%s/pedidos/%d/pagamento", api, id), "")
			var c struct {
				URL string `json:"checkout_url"`
			}
			json.Unmarshal(b, &c)
			u = c.URL
			return st == 200 && u != ""
		})
		return u
	}
	quantidade := func() int {
		_, b := chamar("GET", api+"/produtos", "")
		var products []struct {
			ID  int `json:"product_id"`
			Qtd int `json:"quantity"`
		}
		json.Unmarshal(b, &products)
		for _, p := range products {
			if p.ID == 1 {
				return p.Qtd
			}
		}
		return 0
	}
	aprovado := criar(1)
	url := checkout(aprovado)
	parar("pagamento")
	iniciar("pagamento")
	parar("mock-pagamento")
	iniciar("mock-pagamento")
	aguardar("mock recuperou checkout", func() bool { st, _ := chamar("GET", url, ""); return st == 200 })
	aguardar("pagamento recuperou cobranca", func() bool {
		st, _ := chamar("GET", fmt.Sprintf("http://%s/pagamentos/%d", pay, aprovado), "")
		return st == 200
	})
	resp, err := cliente.Get(fmt.Sprintf("%s/sse/pedidos/%d", api, aprovado))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for range 2 {
		if st, b := chamar("POST", url, "status=APROVADO"); st != 303 {
			t.Fatalf("aprovar: %d %s", st, b)
		}
	}
	aguardar("pedido enviado", func() bool { return estado(aprovado) == base.PedidoEnviado })
	scanner := bufio.NewScanner(resp.Body)
	var eventos []string
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			eventos = append(eventos, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(eventos) != 4 || eventos[3] != "data: pedido.enviado" {
		t.Fatal(eventos)
	}
	if quantidade() != 14 {
		t.Fatal("baixa duplicada")
	}
	t.Log("aprovacao, SSE e reinicio de Pagamento/Mock: OK")
	recusado := criar(1)
	url = checkout(recusado)
	chamar("POST", url, "status=RECUSADO")
	aguardar("recusa e compensacao", func() bool { return estado(recusado) == base.PagamentoRecusado && quantidade() == 14 })
	semEstoque := criar(1000)
	aguardar("estoque insuficiente", func() bool { return estado(semEstoque) == base.EstoqueIndisponivel && quantidade() == 14 })
	cancelado := criar(1)
	checkout(cancelado)
	st, b := chamar("POST", fmt.Sprintf("%s/pedidos/%d/cancelar", api, cancelado), "")
	if st != 202 {
		t.Fatalf("cancelar %d %s", st, b)
	}
	aguardar("cancelamento coordenado", func() bool { return estado(cancelado) == base.PagamentoRecusado && quantidade() == 14 })
	parar("principal")
	iniciar("principal")
	aguardar("principal recuperado", func() bool { return estado(aprovado) == base.PedidoEnviado })
	novo := criar(1000)
	if novo <= cancelado {
		t.Fatal("ID reutilizado")
	}
	aguardar("novo pedido apos reinicio", func() bool { return estado(novo) == base.EstoqueIndisponivel })
	st, b = chamar("POST", api+"/interesse", `{"email":"teste@example.invalid","categorias":["A","B","C"]}`)
	if st != 202 {
		t.Fatalf("interesse %d %s", st, b)
	}
	lerPromocoes := func() map[string]json.RawMessage {
		b, _ := os.ReadFile(filepath.Join(tmp, "promocoes.json"))
		var v map[string]json.RawMessage
		json.Unmarshal(b, &v)
		return v
	}
	aguardar("interesse e notificacoes persistidas", func() bool {
		v := lerPromocoes()
		return bytes.Contains(v["inscricoes"], []byte("teste@example.invalid")) && len(v["pendentes"]) > 2
	})
	parar("promocoes")
	indisponivel.Store(false)
	iniciar("promocoes")
	aguardar("retomada email apos reinicio", func() bool { return envios.Load() > 0 })
	st, _ = chamar("DELETE", api+"/interesse", `{"email":"teste@example.invalid"}`)
	if st != 202 {
		t.Fatal(st)
	}
	aguardar("cancelamento interesse", func() bool {
		v := lerPromocoes()
		return bytes.Contains(v["inscricoes"], []byte(`"categorias_ativas": null`)) && len(v["pendentes"]) == 2
	})
	n := envios.Load()
	time.Sleep(350 * time.Millisecond)
	if envios.Load() != n {
		t.Fatal("e-mail enviado apos cancelamento aplicado")
	}
	e, _ := base.CriarEvento(base.InteressePromocao, "principal", base.InteressePromocaoPayload{Email: "forjado@example.invalid", Categorias: []string{"A"}, Acao: "cadastrar", Versao: 1})
	body, _ := base.SerializarEvento(e)
	if err := ch.PublishWithContext(context.Background(), rabbitmq.ExchangeEcommerce, base.InteressePromocao, false, false, amqp.Publishing{Body: body}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if bytes.Contains(lerPromocoes()["inscricoes"], []byte("forjado")) {
		t.Fatal("interesse sem assinatura aceito")
	}
	t.Log("recusa, falta de estoque, cancelamento, persistencia, promocoes e rejeicao de assinatura: OK")
	t.Run("Firefox", func(t *testing.T) { testarNavegador(t, api) })
}
