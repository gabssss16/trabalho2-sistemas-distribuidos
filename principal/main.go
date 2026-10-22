package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"net/http"
	"os"
	"time"
	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
)

const FilaPrincipal = "fila.principal"

// mapa usado pra associar o tipo de evento com o nome do produtor que gerou o evento. 
// e vincular fila às routing keys corretas.
var produtores = map[string]string{base.EstoqueOK: "estoque", base.EstoqueIndisponivel: "estoque", base.PagamentoAprovado: "pagamento", base.PagamentoRecusado: "pagamento", base.PedidoEnviado: "entrega"}

func config(nome, padrao string) string {
	if v := os.Getenv(nome); v != "" {
		return v
	}
	return padrao
}
func main() {
	s := novoPrincipal()
	s.estoqueURL = config("ESTOQUE_URL", "http://localhost:8081")
	s.pagamentoURL = config("PAGAMENTO_URL", "http://localhost:8082")
	conn := rabbitmq.Conectar()
	defer conn.Close()
	ch := rabbitmq.AbrirCanal(conn)
	defer ch.Close()
	rabbitmq.DeclararTopologiaEcommerce(ch)
	ConfigurarFilaPrincipal(ch)
	s.publicar = rabbitmq.NovoPublicadorDeEventos(conn, "principal", "principal/keys/principal_private.pem")
	rabbitmq.ConsumirConfirmado(ch, FilaPrincipal, s.tratarEvento)
	addr := config("PRINCIPAL_ADDR", ":8080")
	log.Printf("[PRINCIPAL] Loja e API em %s", addr)
	log.Fatal((&http.Server{Addr: addr, Handler: s.rotas(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
func ConfigurarFilaPrincipal(ch *amqp.Channel) {
	rabbitmq.DeclararFila(ch, FilaPrincipal)
	for tipo := range produtores {
		rabbitmq.VincularFila(ch, FilaPrincipal, tipo, rabbitmq.ExchangeEcommerce)
	}
}
