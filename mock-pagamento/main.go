// Mock externo: cria checkout e notifica Pagamento via webhook HTTP.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"trabalho2-sistemas-distribuidos/base"
)

type solicitacao struct {
	base.OrderPayload
	WebhookURL string `json:"webhook_url"`
}

type cobranca struct {
	Solicitacao solicitacao `json:"solicitacao"`
	Chave       string      `json:"idempotency_key"`
	ID          string      `json:"charge_id"`
	Status      string      `json:"status"`
	Notificado  bool        `json:"notificado"`
}

type mockPagamento struct {
	mu                  sync.Mutex
	envioMu             sync.Mutex
	baseURL, webhookURL string
	cobrancas           map[string]cobranca
	cliente             *http.Client
}

func config(nome, padrao string) string {
	if v := os.Getenv(nome); v != "" {
		return v
	}
	return padrao
}

func main() {
	s, err := abrirMock(config("MOCK_PUBLIC_URL", "http://localhost:8083"), config("PAGAMENTO_WEBHOOK_URL", "http://localhost:8082/webhook"))
	if err != nil {
		log.Fatal(err)
	}
	addr := config("MOCK_ADDR", ":8083")
	log.Printf("[MOCK] Checkout em %s; HTTP em %s", s.baseURL, addr)
	log.Fatal((&http.Server{Addr: addr, Handler: s.rotas(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}

func urlHTTP(valor string) bool {
	u, err := url.Parse(valor)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func abrirMock(baseURL, webhookURL string) (*mockPagamento, error) {
	if !urlHTTP(baseURL) || !urlHTTP(webhookURL) {
		return nil, fmt.Errorf("URLs do mock/webhook inválidas")
	}
	u, _ := url.Parse(baseURL)
	if u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("MOCK_PUBLIC_URL deve conter apenas origem HTTP(S)")
	}
	s := &mockPagamento{baseURL: strings.TrimRight(baseURL, "/"), webhookURL: webhookURL, cobrancas: make(map[string]cobranca), cliente: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	return s, nil
}

func validarSolicitacao(s solicitacao, webhookURL string) error {
	if s.OrderID <= 0 || len(s.Items) == 0 {
		return fmt.Errorf("pedido deve conter order_id positivo e itens")
	}
	for _, item := range s.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 {
			return fmt.Errorf("produto e quantidade devem ser positivos")
		}
	}
	// O mock só notifica o endpoint de Pagamento configurado, sem aceitar
	// destinos arbitrários ou redirecionamentos vindos da requisição.
	if s.WebhookURL != webhookURL {
		return fmt.Errorf("webhook_url deve corresponder a PAGAMENTO_WEBHOOK_URL")
	}
	return nil
}

func (s *mockPagamento) rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cobrancas", s.criar)
	mux.HandleFunc("GET /checkout/{id}", s.checkout)
	mux.HandleFunc("POST /checkout/{id}", s.decidir)
	return mux
}

func responderJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *mockPagamento) criar(w http.ResponseWriter, r *http.Request) {
	var req solicitacao
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "JSON inválido", 400)
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		http.Error(w, "esperado um único objeto JSON", 400)
		return
	}
	if err := validarSolicitacao(req, s.webhookURL); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	chave := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if chave == "" || len(chave) > 200 {
		http.Error(w, "Idempotency-Key obrigatória (até 200 caracteres)", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.cobrancas {
		if c.Chave == chave || c.Solicitacao.OrderID == req.OrderID {
			if c.Chave != chave || !reflect.DeepEqual(c.Solicitacao, req) {
				http.Error(w, "chave ou pedido já usado com outros dados", 409)
				return
			}
			s.responderCobranca(w, 200, c)
			return
		}
	}
	var aleatorio [16]byte
	if _, err := rand.Read(aleatorio[:]); err != nil {
		http.Error(w, "falha ao criar identificador", 500)
		return
	}
	c := cobranca{Solicitacao: req, Chave: chave, ID: hex.EncodeToString(aleatorio[:]), Status: "PENDENTE"}
	s.cobrancas[c.ID] = c
	s.responderCobranca(w, 201, c)
}

func (s *mockPagamento) responderCobranca(w http.ResponseWriter, status int, c cobranca) {
	responderJSON(w, status, map[string]string{"charge_id": c.ID, "checkout_url": s.baseURL + "/checkout/" + c.ID})
}

var pagina = template.Must(template.New("checkout").Parse(`<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Simulação de pagamento</title>
<style>body{font:18px system-ui;background:#f3f5f8;color:#17243b;margin:0;padding:24px}main{max-width:540px;margin:6vh auto;background:white;padding:32px;border-radius:16px}h1{font-size:28px}button{font:inherit;padding:14px 18px;border:0;border-radius:8px;cursor:pointer;background:#166534;color:white;margin:8px 8px 8px 0}.recusar{background:#b91c1c}li{margin:12px 0}.aviso{color:#526078}</style></head>
<body><main><p class="aviso">Ambiente de demonstração · Nenhum valor será cobrado</p><h1>Pedido #{{.Solicitacao.OrderID}}</h1>
<ul>{{range .Solicitacao.Items}}<li>Produto {{.ProductID}} — {{.Quantity}} unidade(s)</li>{{end}}</ul><p>Status: <strong>{{.Status}}</strong></p>
{{if eq .Status "PENDENTE"}}<form method="post"><button name="status" value="APROVADO">Pagamento Aprovado</button><button class="recusar" name="status" value="RECUSADO">Pagamento Recusado</button></form>
{{else}}{{if .Notificado}}<p>Resultado comunicado. Você pode voltar à loja.</p>{{else}}<p>Não foi possível comunicar o resultado. Clique abaixo para tentar novamente.</p><form method="post"><button name="status" value="{{.Status}}">Tentar comunicar novamente</button></form><p><a href="">Atualizar status</a></p>{{end}}{{end}}
</main></body></html>`))

func (s *mockPagamento) checkout(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	c, ok := s.cobrancas[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := pagina.Execute(w, c); err != nil {
		log.Printf("[MOCK] Página: %v", err)
	}
}

func (s *mockPagamento) decidir(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "formulário inválido", 400)
		return
	}
	status := r.PostForm.Get("status")
	if status != "APROVADO" && status != "RECUSADO" {
		http.Error(w, "status inválido", 400)
		return
	}
	id := r.PathValue("id")
	s.mu.Lock()
	c, ok := s.cobrancas[id]
	if !ok {
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if c.Status != "PENDENTE" && c.Status != status {
		s.mu.Unlock()
		http.Error(w, "cobrança já finalizada com outro status", 409)
		return
	}
	if c.Status == "PENDENTE" {
		c.Status = status
		s.cobrancas[id] = c
	}
	s.mu.Unlock()
	if err := s.notificar(r.Context(), id); err != nil {
		log.Printf("[MOCK] Webhook pendente de %s: %v", id, err)
	}
	http.Redirect(w, r, "/checkout/"+id, http.StatusSeeOther)
}

func (s *mockPagamento) notificar(ctx context.Context, id string) error {
	// Serializa envios, mas libera o estado durante o HTTP para não bloquear
	// criação de cobranças nem gerar deadlock com o MS Pagamento.
	s.envioMu.Lock()
	defer s.envioMu.Unlock()
	s.mu.Lock()
	c, ok := s.cobrancas[id]
	s.mu.Unlock()
	if !ok || c.Notificado || c.Status == "PENDENTE" {
		return nil
	}
	body, err := json.Marshal(struct {
		OrderID  int    `json:"order_id"`
		ChargeID string `json:"charge_id"`
		Status   string `json:"status"`
	}{c.Solicitacao.OrderID, c.ID, c.Status})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Solicitacao.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.cliente.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook HTTP %d", resp.StatusCode)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Notificado = true
	s.cobrancas[id] = c
	return nil
}
