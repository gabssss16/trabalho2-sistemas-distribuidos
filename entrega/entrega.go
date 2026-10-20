package main

import (
	"fmt"
	"log"
	"sync"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type servicoEntrega struct {
	mu       sync.Mutex
	enviados map[int]bool
	publicar func(string, base.OrderPayload) error
}

func validarPedido(p base.OrderPayload) error {
	if p.OrderID <= 0 || len(p.Items) == 0 {
		return fmt.Errorf("%w: pedido inválido", rabbitmq.ErrEventoInvalido)
	}
	for _, item := range p.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 {
			return fmt.Errorf("%w: item inválido", rabbitmq.ErrEventoInvalido)
		}
	}
	return nil
}
func (s *servicoEntrega) enviar(p base.OrderPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enviados[p.OrderID] {
		return nil
	}
	log.Printf("[ENTREGA] Pedido %d: nota NF-SIM-%d e despacho ENVIO-SIM-%d (simulação)", p.OrderID, p.OrderID, p.OrderID)
	if err := s.publicar(base.PedidoEnviado, p); err != nil {
		return err
	}
	s.enviados[p.OrderID] = true
	return nil
}
