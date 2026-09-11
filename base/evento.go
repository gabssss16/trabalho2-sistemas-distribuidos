package base

import "encoding/json"

const (
	PedidoCriado        = "pedido.criado"
	EstoqueOK           = "pedido.estoque_ok"
	EstoqueIndisponivel = "estoque.indisponivel"
	PagamentoAprovado   = "pagamento.aprovado"
	PagamentoRecusado   = "pagamento.recusado"
	PedidoExcluido      = "pedido.excluido"
	PedidoEnviado       = "pedido.enviado"
)

type OrderItem struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type OrderPayload struct {
	OrderID int         `json:"order_id"`
	Items   []OrderItem `json:"items,omitempty"`
}

type Event struct {
	Type      string          `json:"type"`
	Producer  string          `json:"producer"`
	Payload   json.RawMessage `json:"payload"`
	Signature string          `json:"signature"`
}

func CriarEvento(eventType string, producer string, payload any) (Event, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}

	return Event{
		Type:      eventType,
		Producer:  producer,
		Payload:   payloadBytes,
		Signature: "",
	}, nil
}

func SerializarEvento(event Event) ([]byte, error) {
	return json.Marshal(event)
}

func DesserializarEvento(data []byte) (Event, error) {
	var event Event

	err := json.Unmarshal(data, &event)
	if err != nil {
		return Event{}, err
	}

	return event, nil
}

func DesserializarPayload(event Event, destination any) error {
	return json.Unmarshal(event.Payload, destination)
}
