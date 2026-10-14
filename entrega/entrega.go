package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/persistencia"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type entrega struct {
	Pedido     base.OrderPayload `json:"pedido"`
	NotaFiscal string            `json:"nota_fiscal"`
	Rastreio   string            `json:"rastreio"`
	EnviadoEm  time.Time         `json:"enviado_em"`
	Publicado  bool              `json:"published"`
}

type servicoEntrega struct {
	mu       sync.Mutex
	caminho  string
	entregas map[int]entrega
	publicar func(string, base.OrderPayload) error
}

func validarPedido(p base.OrderPayload) error {
	if p.OrderID <= 0 || len(p.Items) == 0 {
		return fmt.Errorf("%w: pedido sem identificador ou itens", rabbitmq.ErrEventoInvalido)
	}
	for _, item := range p.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 {
			return fmt.Errorf("%w: item inválido", rabbitmq.ErrEventoInvalido)
		}
	}
	return nil
}

func abrirEntrega(caminho string) (*servicoEntrega, error) {
	s := &servicoEntrega{caminho: caminho, entregas: make(map[int]entrega)}
	b, err := os.ReadFile(caminho)
	if errors.Is(err, os.ErrNotExist) {
		return s, persistencia.Salvar(caminho, s.entregas)
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &s.entregas); err != nil {
		return nil, err
	}
	if s.entregas == nil {
		return nil, fmt.Errorf("arquivo de entregas inválido")
	}
	for id, e := range s.entregas {
		if id != e.Pedido.OrderID || validarPedido(e.Pedido) != nil || e.NotaFiscal == "" || e.Rastreio == "" || e.EnviadoEm.IsZero() {
			return nil, fmt.Errorf("entrega %d inválida no arquivo", id)
		}
	}
	return s, nil
}

// O chamador mantém o mutex. Só troca a memória após a gravação atômica.
func (s *servicoEntrega) salvar(e entrega) error {
	proximo := make(map[int]entrega, len(s.entregas)+1)
	for id, existente := range s.entregas {
		proximo[id] = existente
	}
	proximo[e.Pedido.OrderID] = e
	if err := persistencia.Salvar(s.caminho, proximo); err != nil {
		return err
	}
	s.entregas = proximo
	return nil
}

func (s *servicoEntrega) enviar(p base.OrderPayload) error {
	if err := validarPedido(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, existe := s.entregas[p.OrderID]
	if existe {
		if !reflect.DeepEqual(e.Pedido, p) {
			return fmt.Errorf("%w: order_id reutilizado", rabbitmq.ErrEventoInvalido)
		}
		if e.Publicado {
			return nil
		}
	} else {
		// Simulação local: registra uma única nota e despacho por pedido.
		e = entrega{Pedido: p, NotaFiscal: fmt.Sprintf("NF-SIM-%d", p.OrderID), Rastreio: fmt.Sprintf("ENVIO-SIM-%d", p.OrderID), EnviadoEm: time.Now().UTC()}
		if err := s.salvar(e); err != nil {
			return err
		}
		log.Printf("[ENTREGA] Pedido %d: nota %s emitida e despacho %s registrado (simulação)", p.OrderID, e.NotaFiscal, e.Rastreio)
	}
	if err := s.publicar(base.PedidoEnviado, e.Pedido); err != nil {
		return err
	}
	e.Publicado = true
	return s.salvar(e)
}
