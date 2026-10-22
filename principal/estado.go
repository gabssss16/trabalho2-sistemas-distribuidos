package main

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
	"trabalho2-sistemas-distribuidos/base"
)

type Pedido struct {
	Itens     []base.OrderItem
	Status    string
	Historico []string
}
type servicoPrincipal struct {
	mu                       sync.Mutex
	proximoID                int
	pedidos                  map[int]Pedido
	clientes                 map[int]map[chan struct{}]struct{} //conexoes sse
	publicar                 func(string, any) error
	cliente                  *http.Client
	estoqueURL, pagamentoURL string
}

//construtor
func novoPrincipal() *servicoPrincipal {
	return &servicoPrincipal{
		proximoID: int(time.Now().UnixMilli()), pedidos: make(map[int]Pedido),
		clientes: make(map[int]map[chan struct{}]struct{}),
		cliente:  &http.Client{Timeout: 5 * time.Second},
	}
}
//vai ser usado pra impedir alterações em pedidos ja finalizados e encerrar sse
func pedidoFinalizado(st string) bool {
	return st == base.PedidoExcluido || st == base.EstoqueIndisponivel || st == base.PagamentoRecusado || st == base.PedidoEnviado
}
//validacoes basicas de 0, negativo e muuito grande, pra evitar estouro de int
func validarItens(itens []base.OrderItem) error {
	if len(itens) == 0 {
		return fmt.Errorf("informe ao menos um item")
	}
	quantidades := make(map[int]int)
	for _, i := range itens {
		if i.ProductID <= 0 || i.Quantity <= 0 || quantidades[i.ProductID] > math.MaxInt-i.Quantity {
			return fmt.Errorf("produto/quantidade inválidos ou quantidade excede limite")
		}
		quantidades[i.ProductID] += i.Quantity
	}
	return nil
}
func (s *servicoPrincipal) avisar(id int) {
	for c := range s.clientes[id] {
		select {
		case c <- struct{}{}: //sinal de fumaça
		default:
		}
	}
}
