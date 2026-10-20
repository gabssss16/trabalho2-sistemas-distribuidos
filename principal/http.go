package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/front"
)

type CriarPedidoRequest struct {
	Itens []base.OrderItem `json:"itens"`
}
type InteresseRequest struct {
	Email      string   `json:"email"`
	Categorias []string `json:"categorias"`
}
type PedidoResponse struct {
	OrderID int               `json:"order_id"`
	Status  string            `json:"status"`
	Links   map[string]string `json:"_links"`
}

func respostaPedido(id int, p Pedido) PedidoResponse {
	links := map[string]string{"self": fmt.Sprintf("/pedidos/%d", id), "acompanhar_sse": fmt.Sprintf("/sse/pedidos/%d", id), "pagamento": fmt.Sprintf("/pedidos/%d/pagamento", id)}
	if !pedidoFinalizado(p.Status) && p.Status != base.PagamentoAprovado && p.Status != base.PedidoCancelamentoSolicitado {
		links["cancelar"] = fmt.Sprintf("/pedidos/%d/cancelar", id)
	}
	return PedidoResponse{id, p.Status, links}
}
func responderJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func erroHTTP(w http.ResponseWriter, status int, msg string) {
	responderJSON(w, status, map[string]string{"error": msg})
}
func lerJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("esperado um único JSON")
	}
	return nil
}

// O middleware fica antes do roteamento por metodo, incluindo preflights OPTIONS.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *servicoPrincipal) rotas() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /pedidos", s.criarPedido)
	m.HandleFunc("GET /pedidos/{id}", s.consultarPedido)
	m.HandleFunc("POST /pedidos/{id}/cancelar", s.cancelarPedido)
	m.HandleFunc("GET /pedidos/{id}/pagamento", s.consultarPagamento)
	m.HandleFunc("GET /sse/pedidos/{id}", s.handleSSE)
	m.HandleFunc("GET /produtos", s.listarProdutos)
	m.HandleFunc("POST /interesse", s.registrarInteresse)
	m.HandleFunc("DELETE /interesse", s.cancelarInteresse)
	arquivos := http.FileServer(http.FS(front.Arquivos))
	m.Handle("GET /{$}", arquivos)
	m.Handle("GET /app.js", arquivos)
	return corsMiddleware(m)
}
func (s *servicoPrincipal) criarPedido(w http.ResponseWriter, r *http.Request) {
	var req CriarPedidoRequest
	if lerJSON(w, r, &req) != nil {
		erroHTTP(w, 400, "JSON inválido")
		return
	}
	if err := validarItens(req.Itens); err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.proximoID
	if id == math.MaxInt {
		erroHTTP(w, 503, "limite de identificadores")
		return
	}
	s.proximoID++
	p := Pedido{Itens: req.Itens, Status: base.PedidoCriado, Historico: []string{base.PedidoCriado}}
	// O consumidor só altera o pedido após a liberação do mutex.
	s.pedidos[id] = p
	if err := s.publicar(base.PedidoCriado, base.OrderPayload{OrderID: id, Items: req.Itens}); err != nil {
		delete(s.pedidos, id)
		erroHTTP(w, 503, "falha ao publicar pedido")
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/pedidos/%d", id))
	responderJSON(w, 201, respostaPedido(id, p))
}
func idPedido(r *http.Request) (int, error) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("ID inválido")
	}
	return id, nil
}
func (s *servicoPrincipal) consultarPedido(w http.ResponseWriter, r *http.Request) {
	id, err := idPedido(r)
	if err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	p, ok := s.pedidos[id]
	s.mu.Unlock()
	if !ok {
		erroHTTP(w, 404, "pedido não encontrado")
		return
	}
	responderJSON(w, 200, respostaPedido(id, p))
}
func (s *servicoPrincipal) cancelarPedido(w http.ResponseWriter, r *http.Request) {
	id, err := idPedido(r)
	if err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pedidos[id]
	if !ok {
		erroHTTP(w, 404, "pedido não encontrado")
		return
	}
	if pedidoFinalizado(p.Status) || p.Status == base.PagamentoAprovado {
		erroHTTP(w, 409, "pedido já finalizado ou pagamento aprovado")
		return
	}
	if p.Status != base.PedidoCancelamentoSolicitado {
		if err := s.publicar(base.PedidoCancelamentoSolicitado, base.OrderPayload{OrderID: id, Items: p.Itens}); err != nil {
			erroHTTP(w, 503, "falha ao solicitar cancelamento")
			return
		}
		p.Status = base.PedidoCancelamentoSolicitado
		p.Historico = append(p.Historico, p.Status)
		s.pedidos[id] = p
		s.avisar(id)
	}
	responderJSON(w, 202, respostaPedido(id, p))
}
func (s *servicoPrincipal) listarProdutos(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, strings.TrimRight(s.estoqueURL, "/")+"/produtos")
}
func (s *servicoPrincipal) consultarPagamento(w http.ResponseWriter, r *http.Request) {
	id, err := idPedido(r)
	if err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	p, ok := s.pedidos[id]
	s.mu.Unlock()
	if !ok {
		erroHTTP(w, 404, "pedido não encontrado")
		return
	}
	if pedidoFinalizado(p.Status) && p.Status != base.PedidoEnviado {
		erroHTTP(w, 409, "pedido encerrado")
		return
	}
	s.proxy(w, r, fmt.Sprintf("%s/pagamentos/%d", strings.TrimRight(s.pagamentoURL, "/"), id))
}
func (s *servicoPrincipal) proxy(w http.ResponseWriter, r *http.Request, url string) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		erroHTTP(w, 502, "endereço do serviço inválido")
		return
	}
	resp, err := s.cliente.Do(req)
	if err != nil {
		erroHTTP(w, 503, "serviço temporariamente indisponível")
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 || !json.Valid(body) {
		erroHTTP(w, 502, "resposta inválida do serviço")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}
func (s *servicoPrincipal) registrarInteresse(w http.ResponseWriter, r *http.Request) {
	s.alterarInteresse(w, r, "cadastrar")
}
func (s *servicoPrincipal) cancelarInteresse(w http.ResponseWriter, r *http.Request) {
	s.alterarInteresse(w, r, "cancelar")
}
func (s *servicoPrincipal) alterarInteresse(w http.ResponseWriter, r *http.Request, acao string) {
	var req InteresseRequest
	if lerJSON(w, r, &req) != nil {
		erroHTTP(w, 400, "JSON inválido")
		return
	}
	i, err := base.NormalizarInteresse(base.InteressePromocaoPayload{Email: req.Email, Categorias: req.Categorias, Acao: acao})
	if err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	if err := s.publicar(base.InteressePromocao, i); err != nil {
		erroHTTP(w, 503, "falha ao publicar preferência")
		return
	}
	responderJSON(w, 202, map[string]string{"message": "Preferência enviada para processamento"})
}
