# Pagamento, checkout e entrega

Implementação baseada em `main-t2`, cobrindo os requisitos de MS Pagamento, Mock de Pagamento e MS Entrega do enunciado `Trab2_REST_MOM_SSE_2026_2`.

## Fluxo

1. Estoque publica `pedido.estoque_ok`, assinado pelo produtor `estoque`.
2. Pagamento valida o evento e faz `POST /cobrancas` no mock, informando pedido, itens, `webhook_url` e `Idempotency-Key`.
3. O mock persiste a cobrança e retorna `charge_id` e `checkout_url`. Repetir a solicitação mantém a mesma cobrança, inclusive após reiniciar.
4. O navegador abre o checkout. O usuário escolhe **Pagamento Aprovado** ou **Pagamento Recusado**.
5. O mock salva a decisão e envia `POST /webhook` para Pagamento. Se houver falha, tenta novamente a cada cinco segundos e após reiniciar. Uma decisão final não aceita mudança para o estado oposto.
6. Pagamento salva o resultado e publica `pagamento.aprovado` ou `pagamento.recusado` com assinatura RSA. Resultados ainda não publicados são retomados na inicialização e a cada cinco segundos.
7. Entrega consome somente `pagamento.aprovado`, valida assinatura, produtor, routing key e itens. Registra uma nota fiscal e despacho **simulados**, persiste os dados e publica `pedido.enviado` assinado.

A recusa não dispara entrega. O Gateway existente recebe a recusa e publica `pedido.excluido` para liberar a reserva do estoque.

## Executar

Pré-requisitos: Go conforme `go.mod`, RabbitMQ em `localhost:5672` e os pares de chaves dos serviços. Execute a partir da raiz do repositório, em um terminal por serviço, e aguarde todos estarem prontos antes de criar pedidos:

```bash
go run ./entrega
```

```bash
go run ./estoque
```

```bash
go run ./mock-pagamento
```

```bash
go run ./pagamento
```

```bash
go run ./principal
```

Os comandos executam pacotes completos; Entrega agora contém mais de um arquivo Go. Use uma instância de cada serviço por arquivo de dados. Se uma porta já estiver ocupada, pare a instância anterior ou configure outra porta e suas URLs correspondentes.

| Variável | Padrão | Serviço |
| --- | --- | --- |
| `PAGAMENTO_ADDR` | `:8082` | Pagamento: endereço HTTP |
| `PAGAMENTO_DATA_FILE` | `pagamento/data/pagamentos.json` | Pagamento: estado persistido |
| `MOCK_PAGAMENTO_URL` | `http://localhost:8083/cobrancas` | Pagamento: criação de cobrança |
| `PAGAMENTO_WEBHOOK_URL` | `http://localhost:8082/webhook` | Pagamento e Mock: URL de retorno, configure o mesmo valor em ambos |
| `MOCK_ADDR` | `:8083` | Mock: endereço HTTP |
| `MOCK_PUBLIC_URL` | `http://localhost:8083` | Mock: origem acessível ao navegador, sem caminho/query |
| `MOCK_DATA_FILE` | `mock-pagamento/data/cobrancas.json` | Mock: cobranças e notificações pendentes |
| `ENTREGA_DATA_FILE` | `entrega/data/entregas.json` | Entrega: nota, rastreio, data e publicação |

O mock aceita somente a URL de webhook configurada e não segue redirecionamentos do callback. Em contêineres, configure nomes/endereços acessíveis entre os serviços; a URL pública do checkout precisa ser acessível pelo navegador.

## Teste manual

Crie um pedido no Gateway:

```bash
curl -i http://localhost:8080/pedidos \
  -H 'Content-Type: application/json' \
  -d '{"itens":[{"product_id":1,"quantity":1}]}'
```

Use o `order_id` retornado (o exemplo abaixo usa `1`):

```bash
curl http://localhost:8082/pagamentos/1
```

Enquanto Estoque e Pagamento processam os eventos, essa consulta pode retornar 404. Consulte novamente até receber `checkout_url` e abra essa URL no navegador. Aprovar deve produzir `pagamento.aprovado`, depois `pedido.enviado`. Confira o registro simulado em `entrega/data/entregas.json` e os logs do Principal. Crie outro pedido para testar recusa: ele deve terminar em `pagamento.recusado`, sem registro de entrega.

Para simular falha no callback, crie uma cobrança, pare Pagamento e escolha o resultado no checkout. O mock mostra que o resultado foi salvo e está pendente de comunicação. Reinicie Pagamento e atualize o checkout após alguns segundos: ele deve informar que o resultado foi comunicado.

O contrato JSON completo de criação, consulta e webhook está em [estoque-pagamento.md](estoque-pagamento.md).

## Persistência e mensagens repetidas

Entrega grava a nota simulada, o rastreio e a data antes de publicar. Repetições do mesmo pedido reutilizam esse registro. Um `order_id` reaproveitado com outros itens é rejeitado. Se a publicação falhar, a mensagem de entrada é reenfileirada e a próxima tentativa publica a mesma entrega. A confirmação de consumo ocorre somente após o processamento completo; mensagens inválidas são rejeitadas com log.

As publicações de Pagamento e Entrega são persistentes e aguardam confirmação do RabbitMQ. Isso oferece processamento pelo menos uma vez: uma queda após publicar e antes de gravar a confirmação ainda pode duplicar o evento de saída. Não há transação distribuída nem promessa de publicação exatamente uma vez. As filas dos destinatários devem existir antes de publicar. Se a conexão RabbitMQ encerrar, reinicie o serviço.

Os arquivos JSON são substituídos de forma atômica, ficam fora do Git e são mantidos nos reinícios. Não apague os arquivos de apenas um serviço durante um fluxo em andamento.

## Limites da integração com o restante do trabalho

A branch preserva o Gateway de `main-t2`. Ele ainda precisa implementar consulta de checkout pelo frontend, consulta de produtos e SSE; os links apresentados na criação do pedido não significam que esses endpoints já estejam implementados. A consulta direta a Pagamento acima permite testar estes componentes enquanto essa integração é concluída.

O Gateway também mantém os pedidos e o contador de IDs apenas em memória. Reiniciar o Gateway com históricos persistidos pode reutilizar IDs: a camada de Gateway precisa persistir/gerar identificadores únicos antes de usar esse fluxo continuamente. Frontend e SSE continuam pendentes de integração. O MS Promoções foi implementado em um commit posterior; veja [promocoes.md](promocoes.md) para configurar o e-mail e integrar inscrições/cancelamentos no Gateway.

## Validação automatizada

```bash
go test -race ./...
go vet ./...
go build ./...
```

Os testes usam arquivos temporários, chaves RSA temporárias e servidores HTTP locais. Cobrem checkout, contrato do webhook, aprovação e recusa, dados inválidos, conflitos, persistência, reinício, duplicatas, concorrência na entrega, falhas de gravação e retomada após falhas de comunicação/publicação. Não exigem RabbitMQ e não substituem o teste manual integrado com o broker.
