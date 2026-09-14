package integracao

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type logSeguro struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logSeguro) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}
func (l *logSeguro) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }

func aguardar(t *testing.T, descricao string, pronto func() bool) {
	t.Helper()
	limite := time.Now().Add(5 * time.Second)
	for time.Now().Before(limite) {
		if pronto() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tempo esgotado: %s", descricao)
}

// Este teste usa processos reais. Estoque é simulado, pois a feat-seg não foi integrada.
// RABBITMQ_TEST_URL deve apontar para um broker descartável (scripts/teste_integracao.py).
func TestFluxoRabbitMQ(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("execute python3 scripts/teste_integracao.py para usar um broker isolado")
	}
	pasta := t.TempDir()
	raiz, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	// Apenas os binários de teste usam uma cópia temporária da conexão,
	// apontando para o broker isolado. O código do projeto não é alterado.
	caminhoConexao := filepath.Join(raiz, "base", "rabbitmq", "conexao.go")
	conexao, err := os.ReadFile(caminhoConexao)
	if err != nil {
		t.Fatal(err)
	}
	const enderecoPadrao = `"amqp://guest:guest@localhost:5672/"`
	if strings.Count(string(conexao), enderecoPadrao) != 1 {
		t.Fatal("endereço padrão não encontrado; revise a conexão de teste")
	}
	conexaoTeste := filepath.Join(pasta, "conexao.go")
	conteudo := strings.Replace(string(conexao), enderecoPadrao, fmt.Sprintf("%q", url), 1)
	if err := os.WriteFile(conexaoTeste, []byte(conteudo), 0600); err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(map[string]any{"Replace": map[string]string{caminhoConexao: conexaoTeste}})
	if err != nil {
		t.Fatal(err)
	}
	caminhoOverlay := filepath.Join(pasta, "overlay.json")
	if err := os.WriteFile(caminhoOverlay, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	servicos := []string{"principal", "pagamento", "entrega", "consumidor-c1", "consumidor-c2", "promocoes"}
	bins := make(map[string]string)
	for i, servico := range servicos {
		bin := filepath.Join(pasta, fmt.Sprintf("bin-%d", i))
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-race", "-overlay", caminhoOverlay, "-o", bin, "./"+servico)
		cmd.Dir = raiz
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("compilar %s: %v\n%s", servico, err, out)
		}
		bins[servico] = bin
	}
	// Distribuição de chaves temporárias compatíveis com o formato usado pelos serviços.
	for _, destino := range []string{"principal", "estoque", "pagamento", "entrega"} {
		if err := os.MkdirAll(filepath.Join(pasta, destino, "keys"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, produtor := range []string{"principal", "estoque", "pagamento", "entrega"} {
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
		if err := os.WriteFile(filepath.Join(pasta, produtor, "keys", produtor+"_private.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privada}), 0600); err != nil {
			t.Fatal(err)
		}
		for _, destino := range []string{"principal", "estoque", "pagamento", "entrega"} {
			if err := os.WriteFile(filepath.Join(pasta, destino, "keys", produtor+"_public.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publica}), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	logs := make(map[string]*logSeguro)
	iniciar := func(servico string) io.WriteCloser {
		t.Helper()
		cmd := exec.Command(bins[servico])
		cmd.Dir = pasta
		logs[servico] = &logSeguro{}
		cmd.Stdout, cmd.Stderr = logs[servico], logs[servico]
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = stdin.Close()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			if strings.Contains(logs[servico].String(), "DATA RACE") {
				t.Errorf("condição de corrida em %s: %s", servico, logs[servico].String())
			}
			if t.Failed() {
				t.Logf("%s:\n%s", servico, logs[servico].String())
			}
		})
		return stdin
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	rabbitmq.DeclararExchangeEcommerce(ch)
	rabbitmq.DeclararExchangePromocoes(ch)
	for _, nome := range []string{"fila.principal", "fila.pagamento", "fila.entrega", "fila_c1", "fila_c2"} {
		rabbitmq.DeclararFila(ch, nome)
	}
	auditoria, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tipo := range []string{base.PedidoCriado, base.PedidoExcluido, base.EstoqueOK, base.EstoqueIndisponivel, base.PagamentoAprovado, base.PagamentoRecusado, base.PedidoEnviado} {
		rabbitmq.VincularFila(ch, auditoria.Name, tipo, rabbitmq.ExchangeEcommerce)
	}
	mensagens, err := ch.Consume(auditoria.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	principal := iniciar("principal")
	for _, nome := range []string{"pagamento", "entrega", "consumidor-c1", "consumidor-c2"} {
		iniciar(nome)
	}
	for _, nome := range []string{"fila.principal", "fila.pagamento", "fila.entrega", "fila_c1", "fila_c2"} {
		aguardar(t, "consumidor de "+nome, func() bool { fila, err := ch.QueueInspect(nome); return err == nil && fila.Consumers == 1 })
	}
	recebidos := make(map[int]map[string]base.Event)
	esperar := func(id int, tipos ...string) base.Event {
		t.Helper()
		limite := time.NewTimer(5 * time.Second)
		defer limite.Stop()
		for {
			for _, tipo := range tipos {
				if evento, ok := recebidos[id][tipo]; ok {
					valida, err := base.ValidarAssinatura(evento, filepath.Join(pasta, "principal", "keys", evento.Producer+"_public.pem"))
					if err != nil || !valida {
						t.Fatalf("assinatura de %s inválida: %v", tipo, err)
					}
					return evento
				}
			}
			select {
			case delivery, ok := <-mensagens:
				if !ok {
					t.Fatal("auditoria desconectada")
				}
				evento, err := base.DesserializarEvento(delivery.Body)
				if err != nil {
					t.Fatal(err)
				}
				var payload base.OrderPayload
				if err := base.DesserializarPayload(evento, &payload); err != nil {
					t.Fatal(err)
				}
				if recebidos[payload.OrderID] == nil {
					recebidos[payload.OrderID] = make(map[string]base.Event)
				}
				recebidos[payload.OrderID][evento.Type] = evento
			case <-limite.C:
				t.Fatalf("pedido %d: não recebeu %v", id, tipos)
			}
		}
	}
	criar := func(id int) base.OrderPayload {
		t.Helper()
		if _, err := io.WriteString(principal, "2\n1\n1\n2\n"); err != nil {
			t.Fatal(err)
		}
		evento := esperar(id, base.PedidoCriado)
		var payload base.OrderPayload
		if err := base.DesserializarPayload(evento, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.OrderID != id || len(payload.Items) != 1 || payload.Items[0].ProductID != 1 || payload.Items[0].Quantity != 2 {
			t.Fatalf("pedido incorreto: %+v", payload)
		}
		return payload
	}
	publicar := func(tipo, produtor string, payload base.OrderPayload) {
		t.Helper()
		evento, err := base.CriarEvento(tipo, produtor, payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.AssinarEvento(&evento, filepath.Join(pasta, produtor, "keys", produtor+"_private.pem")); err != nil {
			t.Fatal(err)
		}
		body, err := base.SerializarEvento(evento)
		if err != nil {
			t.Fatal(err)
		}
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, tipo, body)
	}
	status := func(id int, tipo string) {
		t.Helper()
		aguardar(t, "status no Principal", func() bool {
			return strings.Contains(logs["principal"].String(), fmt.Sprintf("Pedido %d: %s", id, tipo))
		})
	}

	payload := criar(1)
	publicar(base.EstoqueOK, "estoque", payload)
	resultado := esperar(1, base.PagamentoAprovado, base.PagamentoRecusado)
	if resultado.Type == base.PagamentoAprovado {
		esperar(1, base.PedidoEnviado)
		status(1, base.PedidoEnviado)
	} else {
		esperar(1, base.PedidoExcluido)
		status(1, base.PagamentoRecusado)
	}
	t.Log("PASS: criação -> Estoque simulado -> Pagamento real -> resultado", resultado.Type)

	payload = criar(2)
	publicar(base.PagamentoAprovado, "pagamento", payload)
	esperar(2, base.PedidoEnviado)
	status(2, base.PedidoEnviado)
	if !strings.Contains(logs["entrega"].String(), "Nota do pedido 2 emitida") {
		t.Fatal("nota não foi simulada")
	}
	t.Log("PASS: aprovação assinada -> Entrega real -> pedido.enviado -> Principal")

	payload = criar(3)
	publicar(base.PagamentoRecusado, "pagamento", base.OrderPayload{OrderID: 3})
	excluido := esperar(3, base.PedidoExcluido)
	var devolucao base.OrderPayload
	if err := base.DesserializarPayload(excluido, &devolucao); err != nil {
		t.Fatal(err)
	}
	if len(devolucao.Items) != len(payload.Items) || devolucao.Items[0] != payload.Items[0] {
		t.Fatal("exclusão perdeu os itens originais")
	}
	status(3, base.PagamentoRecusado)
	t.Log("PASS: recusa -> exclusão assinada com os itens originais")

	criar(4)
	publicar(base.EstoqueIndisponivel, "estoque", base.OrderPayload{OrderID: 4})
	esperar(4, base.PedidoExcluido)
	status(4, base.EstoqueIndisponivel)
	t.Log("PASS: falta de estoque -> exclusão assinada")

	criar(5)
	if _, err := io.WriteString(principal, "3\n5\n"); err != nil {
		t.Fatal(err)
	}
	esperar(5, base.PedidoExcluido)
	t.Log("PASS: exclusão manual pelo terminal")

	// Roteamento de promoções: mensagens identificáveis para evitar depender do sorteio.
	for _, categoria := range []string{"A", "B", "C"} {
		body := []byte(fmt.Sprintf(`{"produto":"teste-integracao-%s","categoria":"%s","desconto":20}`, categoria, categoria))
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangePromocoes, "promocao.categoria."+categoria, body)
	}
	aguardar(t, "C1 receber A e B", func() bool {
		s := logs["consumidor-c1"].String()
		return strings.Contains(s, "teste-integracao-A") && strings.Contains(s, "teste-integracao-B")
	})
	aguardar(t, "C2 receber A, B e C", func() bool {
		s := logs["consumidor-c2"].String()
		return strings.Contains(s, "teste-integracao-A") && strings.Contains(s, "teste-integracao-B") && strings.Contains(s, "teste-integracao-C")
	})
	time.Sleep(100 * time.Millisecond)
	if strings.Contains(logs["consumidor-c1"].String(), "teste-integracao-C") {
		t.Fatal("C1 recebeu categoria C")
	}
	t.Log("PASS: C1 recebe A/B; C2 recebe A/B/C (sem validação de assinatura nesta branch)")

	filaPromo, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rabbitmq.VincularFila(ch, filaPromo.Name, "promocao.categoria.*", rabbitmq.ExchangePromocoes)
	promos, err := ch.Consume(filaPromo.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	iniciar("promocoes")
	select {
	case d := <-promos:
		var promo struct {
			Produto   string
			Categoria string
			Desconto  int
		}
		if err := json.Unmarshal(d.Body, &promo); err != nil {
			t.Fatal(err)
		}
		if promo.Produto == "" || !strings.Contains("ABC", promo.Categoria) || len(promo.Categoria) != 1 || promo.Desconto < 10 || promo.Desconto > 49 || d.RoutingKey != "promocao.categoria."+promo.Categoria {
			t.Fatalf("promoção inválida: %s", d.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Promoções não publicou")
	}
	t.Log("PASS: processo Promoções publica categoria e desconto válidos")

}
