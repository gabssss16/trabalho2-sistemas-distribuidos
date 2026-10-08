package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"trabalho2-sistemas-distribuidos/base"
)

func pedidoTeste() base.OrderPayload {
	return base.OrderPayload{OrderID: 7, Items: []base.OrderItem{{ProductID: 1, Quantity: 2}}}
}

func novoPagamento(t *testing.T, mockURL string) *servicoPagamento {
	t.Helper()
	s, err := abrirPagamento(filepath.Join(t.TempDir(), "pagamentos.json"), mockURL, "http://localhost:8082/webhook")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func enviarWebhook(s *servicoPagamento, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.rotas().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body)))
	return rec
}

func TestCriacaoCobrancaContratoPersistenciaEConsulta(t *testing.T) {
	chamadas := 0
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chamadas++
		if r.Method != "POST" || r.URL.Path != "/cobrancas" || r.Header.Get("Idempotency-Key") != "pedido-7" {
			t.Error("requisição incompatível")
		}
		var req struct {
			base.OrderPayload
			WebhookURL string `json:"webhook_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.OrderID != 7 || len(req.Items) != 1 || req.WebhookURL != "http://localhost:8082/webhook" {
			t.Errorf("payload: %+v", req)
		}
		w.WriteHeader(201)
		fmt.Fprint(w, `{"charge_id":"c7","checkout_url":"http://mock/checkout/c7"}`)
	}))
	defer mock.Close()
	s := novoPagamento(t, mock.URL+"/cobrancas")
	if err := s.criarCobranca(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	reiniciado, err := abrirPagamento(s.caminho, s.mockURL, s.webhookURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := reiniciado.criarCobranca(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	if chamadas != 1 {
		t.Fatalf("criou %d cobranças", chamadas)
	}
	rec := httptest.NewRecorder()
	reiniciado.rotas().ServeHTTP(rec, httptest.NewRequest("GET", "/pagamentos/7", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"checkout_url":"http://mock/checkout/c7"`) || !strings.Contains(rec.Body.String(), `"status":"PENDENTE"`) {
		t.Fatalf("consulta: %d %s", rec.Code, rec.Body.String())
	}
}

func TestFalhasDoMockPermitemNovaTentativa(t *testing.T) {
	for _, resposta := range []struct {
		status int
		body   string
	}{
		{503, `{}`}, {201, `invalido`}, {201, `{"charge_id":"c7"}`}, {201, `{"charge_id":"c7","checkout_url":"javascript:alert(1)"}`},
	} {
		t.Run(fmt.Sprint(resposta.status, resposta.body), func(t *testing.T) {
			mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(resposta.status)
				fmt.Fprint(w, resposta.body)
			}))
			defer mock.Close()
			s := novoPagamento(t, mock.URL)
			if err := s.criarCobranca(pedidoTeste()); err == nil {
				t.Fatal("esperado erro do mock")
			}
			if len(s.cobrancas) != 0 {
				t.Fatal("salvou cobrança inválida")
			}
		})
	}
}

func TestWebhookPublicaSomenteResultadoEIgnoraDuplicata(t *testing.T) {
	for status, tipo := range map[string]string{"APROVADO": base.PagamentoAprovado, "RECUSADO": base.PagamentoRecusado} {
		t.Run(status, func(t *testing.T) {
			s := novoPagamento(t, "http://mock/cobrancas")
			if err := s.salvarCobranca(cobranca{Pedido: pedidoTeste(), ChargeID: "c7", Status: "PENDENTE"}); err != nil {
				t.Fatal(err)
			}
			publicacoes := 0
			s.publicar = func(evento string, p base.OrderPayload) error {
				publicacoes++
				if evento != tipo || !pedidosIguais(p, pedidoTeste()) {
					t.Errorf("evento incorreto: %s %+v", evento, p)
				}
				return nil
			}
			body := fmt.Sprintf(`{"order_id":7,"charge_id":"c7","status":%q}`, status)
			for range 2 {
				if rec := enviarWebhook(s, body); rec.Code != 204 {
					t.Fatalf("webhook: %d %s", rec.Code, rec.Body.String())
				}
			}
			reiniciado, err := abrirPagamento(s.caminho, s.mockURL, s.webhookURL)
			if err != nil {
				t.Fatal(err)
			}
			reiniciado.publicar = s.publicar
			if rec := enviarWebhook(reiniciado, body); rec.Code != 204 {
				t.Fatalf("repetição após reinício: %d", rec.Code)
			}
			if publicacoes != 1 {
				t.Fatalf("publicações: %d", publicacoes)
			}
			outro := "APROVADO"
			if status == outro {
				outro = "RECUSADO"
			}
			if rec := enviarWebhook(s, fmt.Sprintf(`{"order_id":7,"charge_id":"c7","status":%q}`, outro)); rec.Code != 409 {
				t.Fatal("aceitou resultados contraditórios")
			}
		})
	}
}

func TestWebhookInvalidoNaoPublica(t *testing.T) {
	s := novoPagamento(t, "http://mock/cobrancas")
	s.cobrancas[7] = cobranca{Pedido: pedidoTeste(), ChargeID: "c7", Status: "PENDENTE"}
	s.publicar = func(string, base.OrderPayload) error { t.Fatal("publicou webhook inválido"); return nil }
	for _, caso := range []struct {
		body   string
		status int
	}{
		{`{`, 400}, {`{} {}`, 400}, {`{"order_id":7,"charge_id":"c7","status":"PENDENTE"}`, 400},
		{`{"order_id":8,"charge_id":"c8","status":"APROVADO"}`, 404},
		{`{"order_id":7,"charge_id":"outro","status":"APROVADO"}`, 409},
		{`{"order_id":7,"status":"APROVADO"}`, 400},
	} {
		if rec := enviarWebhook(s, caso.body); rec.Code != caso.status {
			t.Errorf("%s: %d != %d", caso.body, rec.Code, caso.status)
		}
	}
}

func TestWebhookRetomaPublicacaoAposFalhaEReinicio(t *testing.T) {
	s := novoPagamento(t, "http://mock/cobrancas")
	s.cobrancas[7] = cobranca{Pedido: pedidoTeste(), ChargeID: "c7", Status: "PENDENTE"}
	s.publicar = func(string, base.OrderPayload) error { return fmt.Errorf("broker indisponível") }
	body := `{"order_id":7,"charge_id":"c7","status":"APROVADO"}`
	if rec := enviarWebhook(s, body); rec.Code != 503 {
		t.Fatalf("erro broker: %d", rec.Code)
	}
	reiniciado, err := abrirPagamento(s.caminho, s.mockURL, s.webhookURL)
	if err != nil {
		t.Fatal(err)
	}
	chamadas := 0
	reiniciado.publicar = func(tipo string, p base.OrderPayload) error { chamadas++; return nil }
	if rec := enviarWebhook(reiniciado, body); rec.Code != 204 {
		t.Fatalf("retomada: %d", rec.Code)
	}
	if chamadas != 1 || !reiniciado.cobrancas[7].Publicado {
		t.Fatal("resultado não retomado")
	}
}
