package main

import (
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

// Estrutura que gerencia o estado das entregas
type servicoEntrega struct {
	mu       sync.Mutex
	enviados map[int]bool
	publicar func(string, base.OrderPayload) error
}

// Verifica se os dados do pedido não estão vazios ou zerados
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

// Processa o envio simulado e publica a confirmação
func (s *servicoEntrega) enviar(p base.OrderPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.enviados[p.OrderID] {
		return nil // Evita reenviar se já foi processado
	}
	
	log.Printf("[ENTREGA] Pedido %d: nota NF-SIM-%d e despacho ENVIO-SIM-%d (simulação)", p.OrderID, p.OrderID, p.OrderID)
	
	if err := s.publicar(base.PedidoEnviado, p); err != nil {
		return err
	}
	
	s.enviados[p.OrderID] = true
	return nil
}

// Função principal de inicialização do microsserviço
func main() {
	servico := &servicoEntrega{enviados: make(map[int]bool)}
	
	conn := rabbitmq.Conectar()
	defer conn.Close()
	
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	
	rabbitmq.DeclararTopologiaEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila.entrega")
	rabbitmq.VincularFila(ch, fila.Name, base.PagamentoAprovado, rabbitmq.ExchangeEcommerce)
	
	// Configura a assinatura para as mensagens que saem do Entrega
	servico.publicar = rabbitmq.NovoPublicadorAssinado(conn, "entrega", "entrega/keys/entrega_private.pem")
	
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		pedido, err := lerPagamentoAprovado(d)
		if err != nil {
			return err
		}
		return servico.enviar(pedido)
	})
	
	log.Println("[ENTREGA] Aguardando pagamentos aprovados. CTRL+C para sair.")
	select {} // Mantém o terminal rodando
}

// Desempacota e valida a assinatura do MS Pagamento
func lerPagamentoAprovado(d amqp.Delivery) (base.OrderPayload, error) {
	var pedido base.OrderPayload
	
	evento, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return pedido, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	
	// Valida se a mensagem é realmente de pagamento aprovado
	if evento.Type != base.PagamentoAprovado || d.RoutingKey != base.PagamentoAprovado || evento.Producer != "pagamento" {
		return pedido, fmt.Errorf("%w: esperado pagamento.aprovado do pagamento", rabbitmq.ErrEventoInvalido)
	}
	
	// Valida a assinatura usando a chave pública do MS Pagamento
	ok, err := base.ValidarAssinatura(evento, "entrega/keys/pagamento_public.pem")
	if err != nil || !ok {
		return pedido, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	
	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	
	if err := validarPedido(pedido); err != nil {
		return pedido, err
	}
	
	return pedido, nil
}