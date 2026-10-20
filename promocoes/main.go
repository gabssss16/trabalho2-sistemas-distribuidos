package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/mail"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type Promocao struct {
	Produto   string `json:"produto"`
	Desconto  int    `json:"desconto"`
	Categoria string `json:"categoria"`
}

type mensagemEmail struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}

type clienteEmail struct {
	apiKey, endpoint string
	http             *http.Client
}

type servicoPromocoes struct {
	mu         sync.Mutex
	remetente  string
	inscricoes map[string][]string
	enviar     func(context.Context, mensagemEmail) error
}

var Catalogo = map[string]string{
	"Elden Ring":               "A",
	"Grand Theft Auto V":       "A",
	"Minecraft":                "B",
	"Stardew Valley":           "B",
	"The Witcher 3: Wild Hunt": "C",
	"Red Dead Redemption 2":    "A",
	"Counter-Strike 2":         "C",
	"Baldur's Gate 3":          "B",
}


// LÓGICA DE SORTEIO 

func sortearPromocao() Promocao {
	produtos := make([]string, 0, len(Catalogo))
	for p := range Catalogo {
		produtos = append(produtos, p)
	}

	produtoSorteado := produtos[rand.Intn(len(produtos))]
	categoria := Catalogo[produtoSorteado]

	return Promocao{
		Produto:   produtoSorteado,
		Desconto:  rand.Intn(40) + 10,
		Categoria: categoria,
	}
}

func config(nome, padrao string) string {
	if valor := os.Getenv(nome); valor != "" {
		return valor
	}
	return padrao
}

// INTEGRAÇÃO COM RESEND (email.go)

func novoClienteEmail(apiKey, remetente string) (*clienteEmail, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("configure RESEND_API_KEY para enviar promoções")
	}
	if _, err := mail.ParseAddress(remetente); err != nil {
		return nil, fmt.Errorf("configure PROMOCOES_EMAIL_FROM com um remetente autorizado no Resend")
	}
	return &clienteEmail{
		apiKey:   apiKey,
		endpoint: "https://api.resend.com/emails",
		http: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *clienteEmail) enviar(ctx context.Context, mensagem mensagemEmail) error {
	body, err := json.Marshal(mensagem)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("API de e-mail indisponível: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("API de e-mail retornou HTTP %d", resp.StatusCode)
	}
	var resultado struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&resultado); err != nil || resultado.ID == "" {
		return fmt.Errorf("API de e-mail não confirmou identificador do envio")
	}
	return nil
}


// REGRAS DE NEGÓCIO E ASSINATURA 

func lerInteresse(d amqp.Delivery) (base.InteressePromocaoPayload, error) {
	var i base.InteressePromocaoPayload
	e, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return i, fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if e.Type != base.InteressePromocao || d.RoutingKey != base.InteressePromocao || e.Producer != "principal" {
		return i, fmt.Errorf("%w: esperado interesse.promocao do principal", rabbitmq.ErrEventoInvalido)
	}
	
	// Validação de assinatura 
	ok, err := base.ValidarAssinatura(e, "promocoes/keys/principal_public.pem")
	if err != nil || !ok {
		return i, fmt.Errorf("%w: assinatura inválida", rabbitmq.ErrEventoInvalido)
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
			From:    s.remetente,
			To:      []string{email},
			Subject: fmt.Sprintf("Promoção: %s com %d%% de desconto", p.Produto, p.Desconto),
			Text:    fmt.Sprintf("%s está com %d%% de desconto! Categoria: %s.", p.Produto, p.Desconto, p.Categoria),
		}
		if err := s.enviar(context.Background(), mensagem); err != nil {
			log.Printf("[PROMOÇÕES] Falha no envio para %s: %v", email, err)
		}
	}
}


func main() {
	intervalo, err := time.ParseDuration(config("PROMOCOES_INTERVALO", "5m"))
	if err != nil || intervalo <= 0 {
		log.Fatal("PROMOCOES_INTERVALO deve ser uma duração positiva (ex.: 5m)")
	}
	remetente := os.Getenv("PROMOCOES_EMAIL_FROM")
	email, err := novoClienteEmail(os.Getenv("RESEND_API_KEY"), remetente)
	if err != nil {
		log.Fatal(err)
	}
	servico := &servicoPromocoes{
		remetente:  remetente,
		inscricoes: make(map[string][]string),
		enviar:     email.enviar,
	}

	conn := rabbitmq.Conectar()
	defer conn.Close()
	
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	
	publicador := rabbitmq.AbrirCanal(conn)
	defer publicador.Close()
	
	rabbitmq.DeclararTopologiaEcommerce(ch)
	rabbitmq.DeclararExchangePromocoes(publicador)
	fila := rabbitmq.DeclararFila(ch, "fila.promocoes.interesses")
	rabbitmq.VincularFila(ch, fila.Name, base.InteressePromocao, rabbitmq.ExchangeEcommerce)
	
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		interesse, err := lerInteresse(d)
		if err != nil {
			return err
		}
		return servico.atualizarInteresse(interesse)
	})

	log.Printf("[PROMOÇÕES] Consumindo interesses; novas ofertas a cada %s", intervalo)
	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()
	
	for range ticker.C {
		promocao := sortearPromocao()
		
		// Imprime no terminal para você acompanhar a roleta rodando!
		log.Printf("[PROMOÇÕES] Nova oferta gerada: %s com %d%% de desconto (Categoria %s)", promocao.Produto, promocao.Desconto, promocao.Categoria)
		
		routingKey := "promocao.categoria." + promocao.Categoria
		evento, err := base.CriarEvento(routingKey, "promocoes", promocao)
		if err != nil {
			log.Printf("[PROMOÇÕES] Evento: %v", err)
			continue
		}
		if err := base.AssinarEvento(&evento, "promocoes/keys/promocoes_private.pem"); err != nil {
			log.Printf("[PROMOÇÕES] Assinatura: %v", err)
			continue
		}
		body, err := base.SerializarEvento(evento)
		if err != nil {
			log.Printf("[PROMOÇÕES] Serialização: %v", err)
			continue
		}
		if err := rabbitmq.PublicarEvento(publicador, rabbitmq.ExchangePromocoes, routingKey, body); err != nil {
			log.Printf("[PROMOÇÕES] Falha ao publicar oferta: %v", err)
		}
		
		// Chama o envio de e-mail de verdade
		servico.notificar(promocao)
	}
}