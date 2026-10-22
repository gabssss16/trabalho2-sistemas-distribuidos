package main

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"
)

func (s *servicoPrincipal) handleSSE(w http.ResponseWriter, r *http.Request) {
	id, err := idPedido(r)
	if err != nil {
		erroHTTP(w, 400, err.Error())
		return
	}
	cursor := 0 //guarda o num do ultimo evento enviado pro cliente
	if value := r.Header.Get("Last-Event-ID"); value != "" {
		cursor, err = strconv.Atoi(value)
		if err != nil || cursor < 0 {
			erroHTTP(w, 400, "Last-Event-ID inválido")
			return
		}
	}
	if _, ok := w.(http.Flusher); !ok {
		erroHTTP(w, 500, "streaming indisponível")
		return
	}
	s.mu.Lock()
	p, ok := s.pedidos[id]
	if !ok {
		s.mu.Unlock()
		erroHTTP(w, 404, "pedido não encontrado")
		return
	}
	if cursor > len(p.Historico) {
		s.mu.Unlock()
		erroHTTP(w, 400, "Last-Event-ID além do histórico")
		return
	}
	//cria canal pra conexao e registra no map de clientes 
	//quando o status mudar o avisar vai mandar um sinal pra esse canal 
	c := make(chan struct{}, 1)
	if s.clientes[id] == nil {
		s.clientes[id] = make(map[chan struct{}]struct{})
	}
	s.clientes[id][c] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clientes[id], c)
		if len(s.clientes[id]) == 0 {
			delete(s.clientes, id)
		}
		s.mu.Unlock()
	}()
	//configura o streaming e envia o status
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controle := http.NewResponseController(w)
	//escreve os dados e cama do o flush pra enviar pro cliente
	escrever := func(texto string) error {
		_ = controle.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := fmt.Fprint(w, texto); err != nil {
			return err
		}
		return controle.Flush()
	}
	if err := escrever(": conectado\n\n"); err != nil {
		return
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	//enquanto o pedido nao for finalizado, envia os eventos do historico e espera por novos eventos ou heartbeat
	for {
		s.mu.Lock()
		p = s.pedidos[id]
		historico := slices.Clone(p.Historico)
		s.mu.Unlock()
		for cursor < len(historico) {
			if err := escrever(fmt.Sprintf("id: %d\ndata: %s\n\n", cursor+1, historico[cursor])); err != nil {
				return
			}
			cursor++
		}
		if pedidoFinalizado(p.Status) {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-c:
		//pra nao cortar conexao, envia heartbeat a cada 15s se nao tiver evento novo
		case <-heartbeat.C:
			if escrever(": heartbeat\n\n") != nil {
				return
			}
		}
	}
}
