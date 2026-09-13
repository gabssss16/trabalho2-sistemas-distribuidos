# Verificação dos microsserviços

Verificação realizada na branch `microsservicos-pagamento-entrega`, sem integrar `feat-seg`.

## Executar novamente

Na raiz do projeto, com Go no PATH:

```bash
go test -race ./base/... ./principal ./pagamento ./entrega ./promocoes ./consumidor-c1 ./consumidor-c2 ./tutorial/... ./integracao
python3 scripts/teste_integracao.py
```

O primeiro comando executa os testes unitários e compila os pacotes implementados. A integração é ignorada nesse comando quando `RABBITMQ_TEST_URL` não está definida.

O script requer o RabbitMQ instalado e inicia uma instância temporária em portas locais livres. Compila e executa os serviços reais com detecção de condições de corrida, gera chaves de teste em diretórios temporários e encerra os processos ao terminar. Os dados e as chaves temporárias são removidos. O serviço RabbitMQ já instalado continua funcionando com seus próprios dados.

Se necessário, informe os executáveis:

```bash
python3 scripts/teste_integracao.py --go /caminho/para/go --rabbitmq /usr/lib/rabbitmq/bin/rabbitmq-server
```

Os serviços usam o endereço fixo `amqp://guest:guest@localhost:5672/`, como no tutorial. Para isolar a integração, o teste compila os executáveis usando uma cópia temporária de `conexao.go` com o endereço do broker de teste. Essa substituição ocorre somente na compilação de teste (`go build -overlay`); nenhum arquivo do projeto é reescrito.

## Resultados

Os testes unitários de Principal, Pagamento e Entrega passaram com `-race`. Cobrem aprovação e recusa, preservação dos dados do pedido, assinatura das respostas, descarte de mensagens alteradas, produtor ou routing key incorretos, chaves incorretas e pedidos inválidos. O Principal também impede que um evento de estoque atrasado regrida um pagamento já aprovado.

Sete cenários passaram usando processos reais e um RabbitMQ temporário:

1. Criação pelo terminal do Principal, evento de Estoque simulado e resposta do Pagamento real. O sorteio mantém seu comportamento normal; os dois resultados são cobertos deterministicamente nos testes unitários.
2. Pagamento aprovado simulado e assinado, processamento pela Entrega real e atualização para `pedido.enviado` no Principal.
3. Pagamento recusado simulado e assinado, seguido de `pedido.excluido` assinado pelo Principal, preservando os itens originais mesmo quando a resposta contém apenas o ID.
4. Estoque indisponível simulado e assinado, seguido de exclusão pelo Principal.
5. Exclusão manual pelo terminal, com publicação do evento correspondente.
6. C1 recebendo categorias A/B e C2 recebendo A/B/C, conforme os bindings reais.
7. Promoções gerando produto, categoria, desconto e routing key compatíveis.

## Pendências desta branch

- `go test -race ./...` ainda falha porque `estoque/main.go` e `estoque/estoque.go` estão vazios. O código de Estoque da `feat-seg` não foi integrado nem executado nesses testes; seus eventos foram simulados.
- Promoções publica JSON sem o envelope assinado, e C1/C2 ainda não validam assinaturas. O teste desses processos comprova o roteamento, não o requisito de segurança. A atualização está separada na `feat-seg`.
- As chaves reais ainda não estão nas pastas `keys/` desta branch. Para executar o fluxo assinado fora dos testes, cada produtor precisa de sua chave privada e cada consumidor precisa das chaves públicas correspondentes. Os testes não copiam chaves da outra branch.

A troca de mensagens do tutorial foi compilada, mas não faz parte dos sete cenários de integração acima.
