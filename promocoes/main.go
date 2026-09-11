package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

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
	// Conecta e abre canal usando as ferramentas da pasta base
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	// Garante que a exchange "Promoções" existe antes de publicar
	rabbitmq.DeclararExchangePromocoes(ch)

	log.Println("[*] MS Promoções rodando. Publicando ofertas a cada 5 segundos... CTRL+C para sair")

	// Loop infinito gerando promoções
	for {
		novaPromocao := sortearPromocao()
		routingKey := "promocao.categoria." + novaPromocao.Categoria

		// Converte a struct para formato JSON (bytes)
		body, err := json.Marshal(novaPromocao)
		if err != nil {
			log.Printf("[ERRO] Falha ao converter promoção: %v", err)
			continue
		}

		// Publica na exchange utilizando a routing key do jogo sorteado
		rabbitmq.PublicarEvento(ch, rabbitmq.ExchangePromocoes, routingKey, body)

		// Pausa de 5 segundos antes do próximo sorteio
		time.Sleep(5 * time.Second)
	}
}