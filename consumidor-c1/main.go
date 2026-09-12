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
	fila := rabbitmq.DeclararFila(ch, "fila_c1")

	// C1 se inscreve apenas nas categorias A e B
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.A", rabbitmq.ExchangePromocoes)
	rabbitmq.VincularFila(ch, fila.Name, "promocao.categoria.B", rabbitmq.ExchangePromocoes)

	handler := func(d amqp.Delivery) {
		// 1. Abre o envelope padrão
		evento, err := base.DesserializarEvento(d.Body)
		if err != nil {
			log.Printf("[C1 - ERRO] Falha ao ler envelope: %v", err)
			return
		}

		// 2. Valida a assinatura usando a chave pública do produtor (Promoções)
		valido, err := base.ValidarAssinatura(evento, "consumidor-c1/keys/promocoes_public.pem")
		if err != nil || !valido {
			log.Printf("[C1 - ALERTA] Evento descartado! Assinatura inválida: %v", err)
			return
		}

		// 3. Extrai o conteúdo útil para a struct de Promoção
		var promo Promocao
		err = base.DesserializarPayload(evento, &promo)
		if err != nil {
			log.Printf("[C1 - ERRO] Falha ao ler payload: %v", err)
			return
		}

		log.Printf("[C1] VÁLIDO! Oferta: %s com %d%% OFF (Categoria %s)", promo.Produto, promo.Desconto, promo.Categoria)
	}

	log.Println("[*] Consumidor C1 aguardando promoções A e B... CTRL+C para sair")
	rabbitmq.IniciarConsumo(ch, fila.Name, handler)

	var forever chan struct{}
	<-forever
}