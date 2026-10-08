package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/persistencia"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	loja, err := abrirEstoque(config("ESTOQUE_DATA_FILE", "estoque/data/estoque.json"))
	if err != nil {
		log.Fatal(err)
	}
	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	rabbitmq.DeclararExchangeEcommerce(ch)
	fila := rabbitmq.DeclararFila(ch, "fila_estoque")
	for _, tipo := range []string{base.PedidoCriado, base.PedidoExcluido} {
		rabbitmq.VincularFila(ch, fila.Name, tipo, rabbitmq.ExchangeEcommerce)
	}
	publicar := rabbitmq.NovoPublicadorAssinado(conn, "estoque", "estoque/keys/estoque_private.pem")
	rabbitmq.ConsumirConfirmado(ch, fila.Name, func(d amqp.Delivery) error {
		pedido, tipo, err := lerPedido(d)
		if err != nil {
			return err
		}
		resposta, err := loja.processar(tipo, pedido)
		if err != nil {
			return err
		}
		if resposta != "" {
			return publicar(resposta, pedido)
		}
		return nil
	})
	addr := config("ESTOQUE_ADDR", ":8081")
	log.Printf("[ESTOQUE] HTTP em %s; aguardando pedidos", addr)
	log.Fatal((&http.Server{Addr: addr, Handler: loja.rotas(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}

func lerPedido(d amqp.Delivery) (base.OrderPayload, string, error) {
	var pedido base.OrderPayload
	evento, err := base.DesserializarEvento(d.Body)
	if err != nil {
		return pedido, "", fmt.Errorf("%w: envelope", rabbitmq.ErrEventoInvalido)
	}
	if evento.Producer != "principal" || evento.Type != d.RoutingKey || (evento.Type != base.PedidoCriado && evento.Type != base.PedidoExcluido) {
		return pedido, "", fmt.Errorf("%w: tipo ou produtor", rabbitmq.ErrEventoInvalido)
	}
	ok, err := base.ValidarAssinatura(evento, "estoque/keys/principal_public.pem")
	if err != nil || !ok {
		return pedido, "", fmt.Errorf("%w: assinatura", rabbitmq.ErrEventoInvalido)
	}
	if err := base.DesserializarPayload(evento, &pedido); err != nil {
		return pedido, "", fmt.Errorf("%w: payload", rabbitmq.ErrEventoInvalido)
	}
	return pedido, evento.Type, nil
}

func config(nome, padrao string) string {
	if valor := os.Getenv(nome); valor != "" {
		return valor
	}
	return padrao
}

type produto struct {
	ID         int    `json:"product_id"`
	Nome       string `json:"name"`
	Quantidade int    `json:"quantity"`
}

type reserva struct {
	Itens  []base.OrderItem `json:"items"`
	Status string           `json:"status"`
}

type estadoEstoque struct {
	Produtos map[int]produto `json:"products"`
	Pedidos  map[int]reserva `json:"orders"`
}

type estoquePersistente struct {
	mu      sync.Mutex
	caminho string
	estado  estadoEstoque
}

func abrirEstoque(caminho string) (*estoquePersistente, error) {
	s := &estoquePersistente{caminho: caminho}
	conteudo, err := os.ReadFile(caminho)
	if errors.Is(err, os.ErrNotExist) {
		s.estado = estadoEstoque{Produtos: map[int]produto{
			1: {ID: 1, Nome: "Produto A", Quantidade: 15},
			2: {ID: 2, Nome: "Produto B", Quantidade: 10},
			3: {ID: 3, Nome: "Produto C", Quantidade: 5},
		}, Pedidos: make(map[int]reserva)}
		return s, persistencia.Salvar(caminho, s.estado)
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(conteudo, &s.estado); err != nil {
		return nil, err
	}
	if s.estado.Produtos == nil || s.estado.Pedidos == nil {
		return nil, fmt.Errorf("arquivo de estoque inválido")
	}
	for id, p := range s.estado.Produtos {
		if id <= 0 || p.ID != id || p.Quantidade < 0 {
			return nil, fmt.Errorf("produto persistido inválido: %d", id)
		}
	}
	return s, nil
}

func (s *estoquePersistente) processar(tipo string, pedido base.OrderPayload) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pedido.OrderID <= 0 {
		return "", fmt.Errorf("%w: order_id", rabbitmq.ErrEventoInvalido)
	}
	if tipo != base.PedidoCriado && tipo != base.PedidoExcluido {
		return "", fmt.Errorf("%w: tipo", rabbitmq.ErrEventoInvalido)
	}
	// Copia o estado para manter a memória intacta caso a gravação falhe.
	proximo := estadoEstoque{Produtos: make(map[int]produto), Pedidos: make(map[int]reserva)}
	for id, p := range s.estado.Produtos {
		proximo.Produtos[id] = p
	}
	for id, r := range s.estado.Pedidos {
		proximo.Pedidos[id] = r
	}
	anterior, existe := proximo.Pedidos[pedido.OrderID]
	resposta := ""
	if tipo == base.PedidoExcluido {
		if anterior.Status == base.PedidoExcluido {
			return "", nil
		}
		// Só devolve uma reserva realmente efetuada, usando os itens persistidos.
		if anterior.Status == base.EstoqueOK {
			for _, item := range anterior.Itens {
				p := proximo.Produtos[item.ProductID]
				p.Quantidade += item.Quantity
				proximo.Produtos[item.ProductID] = p
			}
		}
		proximo.Pedidos[pedido.OrderID] = reserva{Status: base.PedidoExcluido}
	} else {
		if existe {
			if anterior.Status == base.PedidoExcluido {
				return "", nil
			}
			if !reflect.DeepEqual(anterior.Itens, pedido.Items) {
				return "", fmt.Errorf("%w: order_id reutilizado com outros itens", rabbitmq.ErrEventoInvalido)
			}
			return anterior.Status, nil
		}
		if len(pedido.Items) == 0 {
			return "", fmt.Errorf("%w: pedido sem itens", rabbitmq.ErrEventoInvalido)
		}
		quantidades := make(map[int]int)
		for _, item := range pedido.Items {
			if item.ProductID <= 0 || item.Quantity <= 0 || quantidades[item.ProductID] > math.MaxInt-item.Quantity {
				return "", fmt.Errorf("%w: produto ou quantidade", rabbitmq.ErrEventoInvalido)
			}
			quantidades[item.ProductID] += item.Quantity
		}
		resposta = base.EstoqueOK
		for id, quantidade := range quantidades {
			if p, existe := proximo.Produtos[id]; !existe || p.Quantidade < quantidade {
				resposta = base.EstoqueIndisponivel
				break
			}
		}
		if resposta == base.EstoqueOK {
			for id, quantidade := range quantidades {
				p := proximo.Produtos[id]
				p.Quantidade -= quantidade
				proximo.Produtos[id] = p
			}
		}
		proximo.Pedidos[pedido.OrderID] = reserva{Itens: append([]base.OrderItem(nil), pedido.Items...), Status: resposta}
	}
	if err := persistencia.Salvar(s.caminho, proximo); err != nil {
		return "", err
	}
	s.estado = proximo
	switch resposta {
	case base.EstoqueOK:
		log.Printf("[ESTOQUE] Pedido %d APROVADO. Estoque atualizado.", pedido.OrderID)
	case base.EstoqueIndisponivel:
		log.Printf("[ESTOQUE] Pedido %d RECUSADO. Falta de estoque.", pedido.OrderID)
	default:
		if anterior.Status == base.EstoqueOK {
			log.Printf("[ESTOQUE] Pedido %d EXCLUÍDO. Itens devolvidos.", pedido.OrderID)
		} else {
			log.Printf("[ESTOQUE] Pedido %d EXCLUÍDO. Sem reserva para devolver.", pedido.OrderID)
		}
	}
	return resposta, nil
}

func (s *estoquePersistente) rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /produtos", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		produtos := make([]produto, 0)
		for _, p := range s.estado.Produtos {
			if p.Quantidade > 0 {
				produtos = append(produtos, p)
			}
		}
		s.mu.Unlock()
		sort.Slice(produtos, func(i, j int) bool { return produtos[i].ID < produtos[j].ID })
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(produtos)
	})
	return mux
}
