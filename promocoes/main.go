package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"os"
	"time"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

// Estrutura básica da promoção
type Promocao struct {
	Produto   string `json:"produto"`
	Desconto  int    `json:"desconto"`
	Categoria string `json:"categoria"`
}

// Catálogo mapeando jogos famosos para suas categorias
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

func sortearPromocao() Promocao {
	// Cria uma lista apenas com os nomes dos jogos
	produtos := make([]string, 0, len(Catalogo))
	for p := range Catalogo {
		produtos = append(produtos, p)
	}

	// Sorteia um jogo aleatório da lista
	produtoSorteado := produtos[rand.Intn(len(produtos))]
	categoria := Catalogo[produtoSorteado]

	return Promocao{
		Produto:   produtoSorteado,
		Desconto:  rand.Intn(40) + 10, // Sorteia desconto entre 10% e 49%
		Categoria: categoria,
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
	servico := &servicoPromocoes{remetente: remetente, inscricoes: make(map[string][]string), enviar: email.enviar}

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
		// Mantém o contrato Topic dos consumidores C1 e C2 do trabalho anterior.
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
		servico.notificar(promocao)
	}
}

func config(nome, padrao string) string {
	if valor := os.Getenv(nome); valor != "" {
		return valor
	}
	return padrao
}
