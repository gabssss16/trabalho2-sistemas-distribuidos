package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"slices"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

func (s *servicoPrincipal) tratarEvento(d amqp.Delivery) error {
	ev, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	produtor, ok := produtores[ev.Type]
	if !ok || ev.Type != d.RoutingKey || ev.Producer != produtor {
		return fmt.Errorf("%w: origem ou tipo", rabbitmq.ErrEventoInvalido)
	}
	valida, err := base.ValidarAssinatura(ev, "principal/keys/"+produtor+"_public.pem")
	if err != nil || !valida {
		return fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	var p base.OrderPayload
	if err := base.DesserializarPayload(ev, &p); err != nil || p.OrderID <= 0 {
		return fmt.Errorf("%w: pedido", rabbitmq.ErrEventoInvalido)
	}
	return s.aplicarEvento(ev.Type, p.OrderID)
}
func (s *servicoPrincipal) aplicarEvento(tipo string, id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.estado.Pedidos[id]
	if !ok {
		return fmt.Errorf("%w: pedido desconhecido", rabbitmq.ErrEventoInvalido)
	}
	if pedidoFinalizado(p.Status) || p.Status == tipo {
		return nil
	}
	if tipo == base.EstoqueOK && (p.Status == base.PagamentoAprovado || p.Status == base.PedidoCancelamentoSolicitado) {
		return nil
	}
	e := s.copiarEstado()
	p = e.Pedidos[id]
	// Envio prova que estoque e pagamento foram aprovados. Normaliza a ordem
	// causal mesmo se produtores diferentes chegarem em outra ordem na fila.
	sucesso := []string{base.EstoqueOK, base.PagamentoAprovado, base.PedidoEnviado}
	if n := slices.Index(sucesso, tipo); n >= 0 {
		for _, st := range sucesso[:n+1] {
			if !slices.Contains(p.Historico, st) {
				p.Historico = append(p.Historico, st)
			}
		}
	} else {
		p.Historico = append(p.Historico, tipo)
	}
	p.Status = tipo
	e.Pedidos[id] = p
	if tipo == base.PagamentoRecusado || tipo == base.EstoqueIndisponivel {
		if err := e.agendar(base.PedidoExcluido, base.OrderPayload{OrderID: id, Items: p.Itens}); err != nil {
			return err
		}
	}
	if err := s.salvar(e); err != nil {
		return err
	}
	s.avisar(id)
	s.sinalizar()
	return nil
}
