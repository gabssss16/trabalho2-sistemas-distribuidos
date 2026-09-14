package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

type Promocao struct {
	Produto   string `json:"produto"`
	Desconto  int    `json:"desconto"`
	Categoria string `json:"categoria"`
}

func main() {
	conn := rabbitmq.Conectar()
	defer conn.Close()

	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()

	rabbitmq.DeclararExchangePromocoes(ch)
	fila := rabbitmq.DeclararFila(ch, "fila_c2")

	// C2 se inscreve em TODAS as categorias usando o curinga
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.*", rabbitmq.ExchangePromocoes)

	handler := func(d amqp.Delivery) {
		evento, err := base.DesserializarEvento(d.Body)
		if err != nil {
			log.Printf("[C2 - ERRO] Falha ao ler envelope: %v", err)
			return
		}

		valido, err := base.ValidarAssinatura(evento, "consumidor-c2/keys/promocoes_public.pem")
		if err != nil || !valido {
			log.Printf("[C2 - ALERTA] Evento descartado! Assinatura inválida: %v", err)
			return
		}

		var promo Promocao
		err = base.DesserializarPayload(evento, &promo)
		if err != nil {
			log.Printf("[C2 - ERRO] Falha ao ler payload: %v", err)
			return
		}

		log.Printf("[C2] VÁLIDO! Oferta: %s com %d%% OFF (Categoria %s)", promo.Produto, promo.Desconto, promo.Categoria)
	}

	log.Println("[*] Consumidor C2 aguardando TODAS as promoções... CTRL+C para sair")
	rabbitmq.IniciarConsumo(ch, fila.Name, handler)

	var forever chan struct{}
	<-forever
}