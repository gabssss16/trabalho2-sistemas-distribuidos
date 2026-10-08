# Estoque e Pagamento — Avaliação 3

Implementação dos dois microsserviços descritos no enunciado `Trab2_REST_MOM_SSE_2026_2`.
O backend continua em Go. Os eventos RabbitMQ mantêm o envelope existente e a assinatura RSA.

## Executar

Na raiz do repositório, com RabbitMQ em `localhost:5672` e as chaves existentes:

```bash
go run ./estoque
```

Em outro terminal, configure o endpoint REST do mock externo e inicie Pagamento:

```bash
MOCK_PAGAMENTO_URL=http://localhost:8083/cobrancas \
PAGAMENTO_WEBHOOK_URL=http://localhost:8082/webhook \
go run ./pagamento
```

A implementação de cada serviço está reunida em seu `main.go`. Na raiz do repositório, também é possível executar `go run estoque/main.go` e `go run pagamento/main.go`.
O Mock de Pagamento é um componente separado: estes comandos não o iniciam.
Gateway REST, frontend, SSE e a página do mock continuam a cargo dos demais componentes do trabalho.
O MS Principal existente ainda utiliza o menu de terminal.

| Variável | Padrão | Uso |
| --- | --- | --- |
| `ESTOQUE_ADDR` | `:8081` | Endereço HTTP de Estoque |
| `ESTOQUE_DATA_FILE` | `estoque/data/estoque.json` | Produtos, reservas e cancelamentos persistidos |
| `PAGAMENTO_ADDR` | `:8082` | Endereço HTTP de Pagamento |
| `PAGAMENTO_DATA_FILE` | `pagamento/data/pagamentos.json` | Cobranças e resultados persistidos |
| `MOCK_PAGAMENTO_URL` | `http://localhost:8083/cobrancas` | URL completa de criação da cobrança |
| `PAGAMENTO_WEBHOOK_URL` | `http://localhost:8082/webhook` | URL que o mock deve chamar |

A URL do webhook precisa ser acessível **a partir do mock**; em contêineres, `localhost` pode apontar para outro processo/ambiente.
Execute uma instância de cada serviço por arquivo JSON. Os diretórios de dados são criados automaticamente e ignorados pelo Git.

## MS Estoque

- Consome `pedido.criado` e `pedido.excluido` na fila `fila_estoque` da exchange `eCommerce`.
- Confere produtor `principal`, routing key, assinatura e dados do pedido.
- Verifica todos os itens antes da baixa. Ocorrências repetidas do mesmo produto são somadas.
- Persiste a baixa e a reserva antes de publicar `pedido.estoque_ok`; publica `estoque.indisponivel` se faltar algum produto ou quantidade.
- Na exclusão, devolve apenas os itens efetivamente reservados. Repetições e pedidos recusados não aumentam o estoque.
- Conserva cancelamentos recebidos antes da criação e não reserva esses pedidos posteriormente.
- Reenvios do mesmo pedido não fazem nova baixa. Um ID já utilizado não aceita outros itens.

Produtos iniciais, criados apenas se o arquivo ainda não existir: Produto A (ID 1, 15 unidades), Produto B (ID 2, 10 unidades) e Produto C (ID 3, 5 unidades).
Arquivos existentes inválidos causam erro de inicialização; não são substituídos por estoque novo.

### Consulta do Gateway

```bash
curl http://localhost:8081/produtos
```

`GET /produtos` retorna HTTP 200 e somente produtos com quantidade disponível maior que zero, ordenados por ID:

```json
[
  {"product_id": 1, "name": "Produto A", "quantity": 15},
  {"product_id": 2, "name": "Produto B", "quantity": 10},
  {"product_id": 3, "name": "Produto C", "quantity": 5}
]
```

Sem produtos disponíveis, retorna `[]`. O Gateway deve consultar esse endpoint via REST.

## MS Pagamento

- Consome `pedido.estoque_ok` na fila `fila.pagamento`, validando produtor `estoque`, routing key e assinatura.
- Solicita uma cobrança via HTTP POST ao mock, incluindo a URL de webhook.
- Guarda a URL de checkout e aguarda o resultado. Não sorteia aprovação/recusa.
- Recebe `APROVADO` ou `RECUSADO` no webhook e publica, respectivamente, `pagamento.aprovado` ou `pagamento.recusado`, com os itens originais e assinatura do MS Pagamento.

### Contrato de integração com o mock

O enunciado define o comportamento, mas não os nomes dos endpoints e campos. Esta implementação adota o contrato abaixo para o componente externo.

Requisição: `POST` para `MOCK_PAGAMENTO_URL`, `Content-Type: application/json`, header `Idempotency-Key: pedido-7`:

```json
{
  "order_id": 7,
  "items": [{"product_id": 1, "quantity": 2}],
  "webhook_url": "http://localhost:8082/webhook"
}
```

Resposta esperada do mock: HTTP 2xx, com identificador textual e URL absoluta HTTP(S):

```json
{
  "charge_id": "cobranca-7",
  "checkout_url": "http://localhost:8083/checkout/cobranca-7"
}
```

O mock deve reutilizar a cobrança para a mesma `Idempotency-Key`. Isso evita duplicação se ocorrer falha entre criar a cobrança remotamente e persistir a resposta localmente.

### Obter a URL de checkout

```bash
curl http://localhost:8082/pagamentos/7
```

`GET /pagamentos/{order_id}` retorna:

```json
{
  "order_id": 7,
  "charge_id": "cobranca-7",
  "checkout_url": "http://localhost:8083/checkout/cobranca-7",
  "status": "PENDENTE"
}
```

Enquanto o evento ainda não gerou uma cobrança, retorna HTTP 404. O Gateway pode consultar novamente para repassar a URL ao frontend. O frontend abre essa URL na nova aba; depois da escolha do usuário, o mock chama o webhook.

### Webhook

O mock envia `POST /webhook` com o pedido, a cobrança correspondente e um dos dois estados finais:

```bash
curl -i http://localhost:8082/webhook \
  -H 'Content-Type: application/json' \
  -d '{"order_id":7,"charge_id":"cobranca-7","status":"APROVADO"}'
```

Para recusar, envie `"status":"RECUSADO"`.

| HTTP | Significado |
| --- | --- |
| 204 | Resultado publicado, ou repetição de resultado já publicado |
| 400 | JSON, identificadores ou status inválidos |
| 404 | Cobrança inexistente |
| 409 | Cobrança não corresponde ao pedido, ou resultado final contraditório |
| 503 | Falha de persistência/publicação: o mock deve repetir o mesmo webhook |

O resultado fica persistido antes da publicação. Se a publicação falhar, o reenvio do webhook retoma a operação, inclusive depois de reiniciar Pagamento.
O endpoint é destinado ao mock no ambiente do trabalho; o `charge_id` correlaciona a cobrança e não constitui autenticação criptográfica do provedor.

## Eventos e garantias

Todos os eventos desses serviços usam o formato já existente:

```json
{
  "type": "pagamento.aprovado",
  "producer": "pagamento",
  "payload": {"order_id": 7, "items": [{"product_id": 1, "quantity": 2}]},
  "signature": "assinatura RSA em Base64"
}
```

As saídas usam mensagens persistentes e confirmação do RabbitMQ. O consumo usa confirmação manual: erros transitórios reenfileiram a mensagem; eventos inválidos são descartados com registro no log.
As filas dos consumidores precisam ser declaradas antes de produzir os eventos, pois o RabbitMQ não armazena mensagens que não tenham uma fila vinculada.
A entrega é pelo menos uma vez: uma queda entre publicar e salvar/confirmar pode repetir o evento; consumidores devem tratar duplicatas. Não há transação distribuída entre arquivo e broker.
Se a conexão com RabbitMQ encerrar, reinicie o serviço. Em falhas de publicação do webhook, o mock precisa repetir a chamada até receber 204.

Os IDs de pedidos precisam ser únicos ao longo dos reinícios. O menu antigo do Principal reinicia sua contagem em 1; o futuro Gateway deve persistir/gerar IDs únicos para trabalhar com o histórico destes serviços.

## Validação

```bash
go test -race ./...
go vet ./...
go build ./...
```

Os testes de Estoque cobrem reinício, reserva e devolução idempotentes, falta de estoque, produtos repetidos, validação, cancelamento antecipado, falha de persistência, concorrência e consulta HTTP.
Os testes de Pagamento usam um servidor HTTP de teste como mock, verificando o contrato REST, persistência da cobrança, consulta do checkout, aprovação/recusa, webhooks inválidos, duplicatas e retomada após falha de publicação.
