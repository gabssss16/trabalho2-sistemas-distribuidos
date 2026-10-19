// Mock externo: cria checkout e notifica Pagamento via webhook HTTP.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
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
	mu         sync.Mutex
	baseURL    string
	webhookURL string
	cobrancas  map[string]cobranca
	cliente    *http.Client
}

func config(nome, padrao string) string {
	if v := os.Getenv(nome); v != "" {
		return v
	}
	return padrao
}

func main() {
	s := &mockPagamento{
		baseURL:    strings.TrimRight(config("MOCK_PUBLIC_URL", "http://localhost:8083"), "/"),
		webhookURL: config("PAGAMENTO_WEBHOOK_URL", "http://localhost:8082/webhook"),
		cobrancas:  make(map[string]cobranca),
		cliente:    &http.Client{Timeout: 5 * time.Second},
	}

	go s.repetirPendentes(context.Background())

	addr := config("MOCK_ADDR", ":8083")
	log.Printf("[MOCK] Checkout em %s; HTTP em %s", s.baseURL, addr)
	log.Fatal((&http.Server{Addr: addr, Handler: s.rotas(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}

func (s *mockPagamento) rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cobrancas", s.criar)
	mux.HandleFunc("GET /checkout/{id}", s.checkout)
	mux.HandleFunc("POST /checkout/{id}", s.decidir)
	return mux
}

func (s *mockPagamento) criar(w http.ResponseWriter, r *http.Request) {
	var req solicitacao
	json.NewDecoder(r.Body).Decode(&req)

	chave := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Verifica se a cobrança já foi solicitada antes (idempotência na RAM)
	for _, c := range s.cobrancas {
		if c.Chave == chave || c.Solicitacao.OrderID == req.OrderID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"charge_id": c.ID, "checkout_url": s.baseURL + "/checkout/" + c.ID})
			return
		}
	}

	var aleatorio [16]byte
	rand.Read(aleatorio[:])
	id := hex.EncodeToString(aleatorio[:])

	// Salva a nova cobrança apenas na memória RAM
	c := cobranca{Solicitacao: req, Chave: chave, ID: id, Status: "PENDENTE"}
	s.cobrancas[id] = c 

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(map[string]string{"charge_id": c.ID, "checkout_url": s.baseURL + "/checkout/" + c.ID})
}

var pagina = template.Must(template.New("checkout").Parse(`<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><title>Simulação de pagamento</title>
<style>body{font:18px system-ui;background:#f3f5f8;padding:24px}main{max-width:540px;margin:6vh auto;background:white;padding:32px;border-radius:16px}button{padding:14px 18px;background:#166534;color:white;border:0;border-radius:8px;cursor:pointer}.recusar{background:#b91c1c}</style></head>
<body><main><h1>Pedido #{{.Solicitacao.OrderID}}</h1><p>Status: <strong>{{.Status}}</strong></p>
{{if eq .Status "PENDENTE"}}<form method="post"><button name="status" value="APROVADO">Pagamento Aprovado</button><button class="recusar" name="status" value="RECUSADO">Pagamento Recusado</button></form>
{{else}}{{if .Notificado}}<p>Resultado comunicado à loja. Você pode fechar esta aba.</p>{{else}}<p>Processando...</p><form method="post"><button name="status" value="{{.Status}}">Tentar enviar novamente</button></form>{{end}}{{end}}
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
	pagina.Execute(w, c)
}

func (s *mockPagamento) decidir(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	status := r.PostForm.Get("status")
	id := r.PathValue("id")
	
	s.mu.Lock()
	c, ok := s.cobrancas[id]
	if !ok {
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	
	if c.Status == "PENDENTE" {
		c.Status = status
		s.cobrancas[id] = c // Atualiza a decisão do usuário na RAM
	}
	s.mu.Unlock()
	
	s.notificar(context.Background(), id)
	http.Redirect(w, r, "/checkout/"+id, http.StatusSeeOther)
}

func (s *mockPagamento) notificar(ctx context.Context, id string) {
	s.mu.Lock()
	c, ok := s.cobrancas[id]
	s.mu.Unlock()
	
	if !ok || c.Notificado || c.Status == "PENDENTE" {
		return
	}
	
	body, _ := json.Marshal(map[string]any{"order_id": c.Solicitacao.OrderID, "charge_id": c.ID, "status": c.Status})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.Solicitacao.WebhookURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := s.cliente.Do(req)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.mu.Lock()
		c.Notificado = true
		s.cobrancas[id] = c // Marca como notificado na RAM para não reenviar Webhook
		s.mu.Unlock()
		resp.Body.Close()
	}
}

func (s *mockPagamento) repetirPendentes(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			var ids []string
			for id, c := range s.cobrancas {
				if c.Status != "PENDENTE" && !c.Notificado {
					ids = append(ids, id)
				}
			}
			s.mu.Unlock()
			for _, id := range ids {
				s.notificar(ctx, id)
			}
		}
	}
}