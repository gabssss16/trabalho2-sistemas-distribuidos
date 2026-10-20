package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func novoMock(t *testing.T, webhook string) *mockPagamento {
	t.Helper()
	s, err := abrirMock(filepath.Join(t.TempDir(), "cobrancas.json"), "http://localhost:8083", webhook)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func criarTeste(s *mockPagamento, chave, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/cobrancas", strings.NewReader(body))
	r.Header.Set("Idempotency-Key", chave)
	w := httptest.NewRecorder()
	s.rotas().ServeHTTP(w, r)
	return w
}

func bodyPedido(webhook string) string {
	return fmt.Sprintf(`{"order_id":7,"items":[{"product_id":1,"quantity":2}],"webhook_url":%q}`, webhook)
}

func criarValida(t *testing.T, s *mockPagamento) string {
	t.Helper()
	w := criarTeste(s, "pedido-7", bodyPedido(s.webhookURL))
	if w.Code != 201 {
		t.Fatalf("criar: %d %s", w.Code, w.Body.String())
	}
	var resultado map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resultado); err != nil {
		t.Fatal(err)
	}
	id := resultado["charge_id"]
	if id == "" || resultado["checkout_url"] != s.baseURL+"/checkout/"+id {
		t.Fatal("contrato da cobrança inválido")
	}
	return id
}

func decidirTeste(s *mockPagamento, id, status string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/checkout/"+id, strings.NewReader(url.Values{"status": {status}}.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.rotas().ServeHTTP(w, r)
	return w
}

func TestCriacaoIdempotentePersistidaEConflitos(t *testing.T) {
	s := novoMock(t, "http://localhost:8082/webhook")
	id := criarValida(t, s)
	reiniciado, err := abrirMock(s.caminho, s.baseURL, s.webhookURL)
	if err != nil {
		t.Fatal(err)
	}
	w := criarTeste(reiniciado, "pedido-7", bodyPedido(s.webhookURL))
	if w.Code != 200 || !strings.Contains(w.Body.String(), id) || len(reiniciado.cobrancas) != 1 {
		t.Fatal("duplicou cobrança após reinício")
	}
	if w := criarTeste(s, "pedido-7", strings.Replace(bodyPedido(s.webhookURL), `"quantity":2`, `"quantity":3`, 1)); w.Code != 409 {
		t.Fatal("aceitou chave com dados diferentes")
	}
	if w := criarTeste(s, "outra-chave", bodyPedido(s.webhookURL)); w.Code != 409 {
		t.Fatal("aceitou segundo pagamento para mesmo pedido")
	}
	w = httptest.NewRecorder()
	s.rotas().ServeHTTP(w, httptest.NewRequest("GET", "/checkout/"+id, nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Pagamento Aprovado") || !strings.Contains(w.Body.String(), "Pagamento Recusado") {
		t.Fatal("checkout não oferece ambas as opções")
	}
}

func TestAprovarERecusarEnviamWebhookUmaVez(t *testing.T) {
	for _, status := range []string{"APROVADO", "RECUSADO"} {
		t.Run(status, func(t *testing.T) {
			var mu sync.Mutex
			chamadas := 0
			var recebido struct {
				OrderID  int    `json:"order_id"`
				ChargeID string `json:"charge_id"`
				Status   string `json:"status"`
			}
			webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				chamadas++
				if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("contrato HTTP incorreto")
				}
				if err := json.NewDecoder(r.Body).Decode(&recebido); err != nil {
					t.Error(err)
				}
				w.WriteHeader(204)
			}))
			defer webhook.Close()
			s := novoMock(t, webhook.URL)
			id := criarValida(t, s)
			for range 2 {
				if w := decidirTeste(s, id, status); w.Code != 303 {
					t.Fatalf("decisão: %d", w.Code)
				}
			}
			mu.Lock()
			if chamadas != 1 || recebido.OrderID != 7 || recebido.ChargeID != id || recebido.Status != status {
				t.Errorf("webhook incorreto: %d %+v", chamadas, recebido)
			}
			mu.Unlock()
			outro := "APROVADO"
			if outro == status {
				outro = "RECUSADO"
			}
			if w := decidirTeste(s, id, outro); w.Code != 409 {
				t.Fatal("alterou resultado final")
			}
			reiniciado, err := abrirMock(s.caminho, s.baseURL, s.webhookURL)
			if err != nil {
				t.Fatal(err)
			}
			if !reiniciado.cobrancas[id].Notificado || reiniciado.cobrancas[id].Status != status {
				t.Fatal("perdeu resultado")
			}
			w := httptest.NewRecorder()
			reiniciado.rotas().ServeHTTP(w, httptest.NewRequest("GET", "/checkout/"+id, nil))
			if !strings.Contains(w.Body.String(), "Resultado comunicado") {
				t.Fatal("checkout não informa sucesso")
			}
		})
	}
}

func TestWebhookPendenteRetomadoAposReinicio(t *testing.T) {
	var mu sync.Mutex
	falhar := true
	chamadas := 0
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		chamadas++
		if falhar {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer webhook.Close()
	s := novoMock(t, webhook.URL)
	id := criarValida(t, s)
	if w := decidirTeste(s, id, "APROVADO"); w.Code != 303 {
		t.Fatal(w.Code)
	}
	if s.cobrancas[id].Status != "APROVADO" || s.cobrancas[id].Notificado {
		t.Fatal("perdeu decisão pendente")
	}
	mu.Lock()
	falhar = false
	mu.Unlock()
	reiniciado, err := abrirMock(s.caminho, s.baseURL, s.webhookURL)
	if err != nil {
		t.Fatal(err)
	}
	reiniciado.retomarPendentes(context.Background())
	reiniciado.retomarPendentes(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if chamadas != 2 || !reiniciado.cobrancas[id].Notificado {
		t.Fatal("não retomou webhook")
	}
}

func TestCriacaoInvalidaNaoPersiste(t *testing.T) {
	s := novoMock(t, "http://localhost:8082/webhook")
	for _, caso := range []struct{ chave, body string }{
		{"pedido-7", `{`}, {"pedido-7", `{ } { }`}, {"pedido-7", `null`},
		{"", bodyPedido(s.webhookURL)}, {"pedido-7", bodyPedido("http://destino-arbitrario/webhook")},
		{"pedido-7", strings.Replace(bodyPedido(s.webhookURL), `"quantity":2`, `"quantity":0`, 1)},
	} {
		if w := criarTeste(s, caso.chave, caso.body); w.Code != 400 {
			t.Errorf("aceitou entrada inválida: %d", w.Code)
		}
	}
	if len(s.cobrancas) != 0 {
		t.Fatal("persistiu entrada inválida")
	}
	if w := decidirTeste(s, "inexistente", "APROVADO"); w.Code != 404 {
		t.Fatal("aceitou cobrança desconhecida")
	}
	if w := decidirTeste(s, "inexistente", "INVALIDO"); w.Code != 400 {
		t.Fatal("aceitou status inválido")
	}
}

func TestFalhaPersistenciaNaoEnviaWebhook(t *testing.T) {
	s := novoMock(t, "http://localhost:8082/webhook")
	id := criarValida(t, s)
	s.caminho = filepath.Join(s.caminho, "invalido.json")
	if w := decidirTeste(s, id, "APROVADO"); w.Code != 503 {
		t.Fatal("ignorou falha ao salvar")
	}
	if s.cobrancas[id].Status != "PENDENTE" {
		t.Fatal("alterou memória sem persistir")
	}
}

func TestArquivoInvalidoNaoSobrescrito(t *testing.T) {
	for _, conteudo := range []string{`null`, `{`, `{"id":{}}`} {
		path := filepath.Join(t.TempDir(), "cobrancas.json")
		if err := os.WriteFile(path, []byte(conteudo), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := abrirMock(path, "http://localhost:8083", "http://localhost:8082/webhook"); err == nil {
			t.Fatal("aceitou arquivo inválido")
		}
	}
}
