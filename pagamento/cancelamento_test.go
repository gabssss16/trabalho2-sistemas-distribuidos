package main

import (
	"sync"
	"testing"
	"trabalho2-sistemas-distribuidos/base"
)

func TestCancelamentoAntesDaCobrancaPersiste(t *testing.T) {
	s := novoPagamento(t, "http://mock/cobrancas")
	s.publicar = func(tipo string, p base.OrderPayload) error {
		if tipo != base.PagamentoRecusado {
			t.Error(tipo)
		}
		return nil
	}
	if err := s.cancelarPedido(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	novo, err := abrirPagamento(s.caminho, s.mockURL, s.webhookURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := novo.criarCobranca(pedidoTeste()); err != nil {
		t.Fatal(err)
	}
	if novo.cobrancas[7].Status != "RECUSADO" {
		t.Fatal("cancelamento perdido")
	}
}
func TestCancelamentoEWebhookConcorrentesNaoContradizem(t *testing.T) {
	for range 10 {
		s := novoPagamento(t, "http://mock/cobrancas")
		s.salvarCobranca(cobranca{Pedido: pedidoTeste(), ChargeID: "c7", Status: "PENDENTE"})
		var eventos []string
		s.publicar = func(tipo string, p base.OrderPayload) error { eventos = append(eventos, tipo); return nil }
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s.cancelarPedido(pedidoTeste()) }()
		go func() { defer wg.Done(); enviarWebhook(s, `{"order_id":7,"charge_id":"c7","status":"APROVADO"}`) }()
		wg.Wait()
		for _, e := range eventos {
			if e != eventos[0] {
				t.Fatal("resultados contraditorios", eventos)
			}
		}
	}
}
