package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"slices"
	"sync"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type mensagemEmail struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}
type servicoPromocoes struct {
	mu         sync.Mutex
	remetente  string
	inscricoes map[string][]string
	enviar     func(context.Context, mensagemEmail) error
}

func lerInteresse(d amqp.Delivery) (base.InteressePromocaoPayload, error) {
	var i base.InteressePromocaoPayload
	e, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return i, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if e.Type != base.InteressePromocao || d.RoutingKey != base.InteressePromocao || e.Producer != "principal" {
		return i, fmt.Errorf("%w: esperado interesse.promocao do principal", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(e, "promocoes/keys/principal_public.pem")
	if err != nil || !ok {
		return i, fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(e, &i); err != nil {
		return i, fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	normalizado, err := base.NormalizarInteresse(i)
	if err != nil {
		return i, fmt.Errorf("%w: %v", rabbitmq.ErrEventoInvalido, err)
	}
	return normalizado, nil
}

func (s *servicoPromocoes) atualizarInteresse(i base.InteressePromocaoPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i.Acao == "cancelar" {
		delete(s.inscricoes, i.Email)
	} else {
		s.inscricoes[i.Email] = slices.Clone(i.Categorias)
	}
	return nil
}

func (s *servicoPromocoes) notificar(p Promocao) {
	s.mu.Lock()
	var destinatarios []string
	for email, categorias := range s.inscricoes {
		if slices.Contains(categorias, p.Categoria) {
			destinatarios = append(destinatarios, email)
		}
	}
	s.mu.Unlock()
	for _, email := range destinatarios {
		mensagem := mensagemEmail{
			From: s.remetente, To: []string{email},
			Subject: fmt.Sprintf("Promoção: %s com %d%% de desconto", p.Produto, p.Desconto),
			Text:    fmt.Sprintf("%s está com %d%% de desconto! Categoria: %s.", p.Produto, p.Desconto, p.Categoria),
		}
		if err := s.enviar(context.Background(), mensagem); err != nil {
			log.Printf("[PROMOÇÕES] Falha no envio: %v", err)
		}
	}
}
