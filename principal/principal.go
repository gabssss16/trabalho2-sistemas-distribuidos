package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

const FilaPrincipal = "fila.principal"

type Pedido struct {
	Itens  []base.OrderItem
	Status string
}

var produtos = []string{"Produto A", "Produto B", "Produto C"}

// A mesma relação define os bindings e identifica quem pode enviar cada evento.
var produtores = map[string]string{
	base.EstoqueOK:           "estoque",
	base.EstoqueIndisponivel: "estoque",
	base.PagamentoAprovado:   "pagamento",
	base.PagamentoRecusado:   "pagamento",
	base.PedidoEnviado:       "entrega",
}

var (
	pedidos   = make(map[int]Pedido)
	proximoID = 1
	mu        sync.Mutex // O menu e o consumidor acessam os mesmos pedidos.
)

func ExecutarMenu(ch *amqp.Channel) {
	entrada := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\n1 - Visualizar produtos\n2 - Realizar pedido\n3 - Excluir pedido\n4 - Consultar pedidos\n0 - Sair")
		opcao, err := lerInteiro(entrada, "Opção: ")
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
		switch opcao {
		case 1:
			ListarProdutos()
		case 2:
			err = CriarPedido(ch, entrada)
		case 3:
			var id int
			id, err = lerInteiro(entrada, "ID do pedido: ")
			if err == nil {
				err = ExcluirPedido(ch, id)
			}
		case 4:
			mu.Lock()
			if len(pedidos) == 0 {
				fmt.Println("Nenhum pedido cadastrado.")
			}
			for id := 1; id < proximoID; id++ {
				pedido := pedidos[id]
				fmt.Printf("Pedido %d | Status: %s\n", id, pedido.Status)
				for _, item := range pedido.Itens {
					fmt.Printf("  Produto %d | Quantidade: %d\n", item.ProductID, item.Quantity)
				}
			}
			mu.Unlock()
		case 0:
			return
		default:
			fmt.Println("Opção inválida.")
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
		}
	}
}

func lerInteiro(entrada *bufio.Scanner, pergunta string) (int, error) {
	fmt.Print(pergunta)
	if !entrada.Scan() {
		if err := entrada.Err(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	n, err := strconv.Atoi(strings.TrimSpace(entrada.Text()))
	if err != nil {
		return 0, fmt.Errorf("informe um número inteiro")
	}
	return n, nil
}

func ListarProdutos() {
	for i, nome := range produtos {
		fmt.Printf("%d - %s\n", i+1, nome)
	}
}

func CriarPedido(ch *amqp.Channel, entrada *bufio.Scanner) error {
	ListarProdutos()
	quantos, err := lerInteiro(entrada, "Quantos itens deseja adicionar? ")
	if err != nil {
		return err
	}
	if quantos <= 0 {
		return fmt.Errorf("o pedido deve ter pelo menos um item")
	}
	var itens []base.OrderItem
	for i := 0; i < quantos; i++ {
		id, err := lerInteiro(entrada, "ID do produto: ")
		if err != nil {
			return err
		}
		if id < 1 || id > len(produtos) {
			return fmt.Errorf("produto não encontrado")
		}
		quantidade, err := lerInteiro(entrada, "Quantidade: ")
		if err != nil {
			return err
		}
		if quantidade <= 0 {
			return fmt.Errorf("a quantidade deve ser positiva")
		}
		itens = append(itens, base.OrderItem{ProductID: id, Quantity: quantidade})
	}

	mu.Lock()
	defer mu.Unlock()
	payload := base.OrderPayload{OrderID: proximoID, Items: itens}
	if err := PublicarEventoPrincipal(ch, base.PedidoCriado, payload); err != nil {
		return err
	}
	pedidos[proximoID] = Pedido{Itens: itens, Status: base.PedidoCriado}
	fmt.Printf("Pedido %d criado.\n", proximoID)
	proximoID++
	return nil
}

func ExcluirPedido(ch *amqp.Channel, id int) error {
	mu.Lock()
	defer mu.Unlock()
	pedido, existe := pedidos[id]
	if !existe {
		return fmt.Errorf("pedido não encontrado")
	}
	if pedidoFinalizado(pedido.Status) {
		return fmt.Errorf("pedido já enviado ou cancelado")
	}
	payload := base.OrderPayload{OrderID: id, Items: pedido.Itens}
	if err := PublicarEventoPrincipal(ch, base.PedidoExcluido, payload); err != nil {
		return err
	}
	pedido.Status = base.PedidoExcluido
	pedidos[id] = pedido
	fmt.Printf("Pedido %d excluído.\n", id)
	return nil
}

func ConfigurarFilaPrincipal(ch *amqp.Channel) {
	rabbitmq.DeclararFila(ch, FilaPrincipal)
	for tipo := range produtores {
		rabbitmq.VincularFila(ch, FilaPrincipal, tipo, rabbitmq.ExchangeEcommerce)
	}
}

func TratarEvento(ch *amqp.Channel, delivery amqp.Delivery) {
	evento, err := base.DesserializarEvento(delivery.Body)
	if err != nil {
		log.Printf("Evento descartado: %v", err)
		return
	}
	produtor, conhecido := produtores[evento.Type]
	if !conhecido || evento.Type != delivery.RoutingKey || evento.Producer != produtor {
		log.Print("Evento descartado: tipo ou produtor incorreto")
		return
	}
	valida, err := base.ValidarAssinatura(evento, "principal/keys/"+produtor+"_public.pem")
	if err != nil || !valida {
		log.Printf("Evento descartado: assinatura inválida ou chave indisponível (%v)", err)
		return
	}
	var payload base.OrderPayload
	if err := base.DesserializarPayload(evento, &payload); err != nil {
		log.Printf("Evento descartado: %v", err)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	pedido, existe := pedidos[payload.OrderID]
	if !existe || pedidoFinalizado(pedido.Status) {
		return
	}
	if evento.Type == base.EstoqueIndisponivel || evento.Type == base.PagamentoRecusado {
		// Usa os itens originais, mesmo quando a resposta contém apenas o ID.
		payload.Items = pedido.Itens
		if err := PublicarEventoPrincipal(ch, base.PedidoExcluido, payload); err != nil {
			log.Printf("Falha ao excluir pedido: %v", err)
			return
		}
	}
	pedido.Status = evento.Type
	pedidos[payload.OrderID] = pedido
	fmt.Printf("\nPedido %d: %s\n", payload.OrderID, pedido.Status)
}

func pedidoFinalizado(status string) bool {
	return status == base.PedidoExcluido || status == base.EstoqueIndisponivel ||
		status == base.PagamentoRecusado || status == base.PedidoEnviado
}

func PublicarEventoPrincipal(ch *amqp.Channel, tipo string, payload base.OrderPayload) error {
	evento, err := base.CriarEvento(tipo, "principal", payload)
	if err != nil {
		return err
	}
	if err := base.AssinarEvento(&evento, "principal/keys/principal_private.pem"); err != nil {
		return err
	}
	body, err := base.SerializarEvento(evento)
	if err != nil {
		return err
	}
	rabbitmq.PublicarEvento(ch, rabbitmq.ExchangeEcommerce, tipo, body)
	return nil
}
