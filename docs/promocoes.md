# MS Promoções: interesses e e-mail

O serviço mantém o catálogo e as categorias A/B/C existentes, consome interesses do Principal e envia ofertas por API externa de e-mail. As mensagens de oferta `promocao.categoria.A`, `.B` e `.C` continuam assinadas e publicadas na exchange Topic `Promoções`, compatíveis com C1/C2. Esses consumidores antigos não são responsáveis pelo envio de e-mails.

## Contrato para o Principal

- Exchange Direct: `eCommerce`.
- Routing key e `type`: `interesse.promocao` (`base.InteressePromocao`).
- Produtor: `principal`.
- Payload compartilhado: `base.InteressePromocaoPayload`, em `base/evento.go`.
- Assinatura: usar `base.AssinarEvento` com `principal/keys/principal_private.pem` antes de serializar/publicar.
- O consumidor usa a fila durável `fila.promocoes.interesses` e valida a assinatura com `promocoes/keys/principal_public.pem`.

Cadastro ou substituição das categorias de interesse:

```json
{
  "email": "ana@example.com",
  "categorias": ["A", "B"],
  "acao": "cadastrar",
  "versao": 1
}
```

Cancelamento de uma categoria, mantendo as demais:

```json
{
  "email": "ana@example.com",
  "categorias": ["A"],
  "acao": "cancelar",
  "versao": 2
}
```

Cancelamento de todos os interesses:

```json
{
  "email": "ana@example.com",
  "acao": "cancelar",
  "versao": 3
}
```

Esses exemplos são **payloads**, não envelopes completos. Não envie o payload diretamente no RabbitMQ: envolva-o no `base.Event` e assine. Exemplo para a integração no Principal:

```go
payload := base.InteressePromocaoPayload{
    Email: "ana@example.com",
    Categorias: []string{"A", "B"},
    Acao: "cadastrar",
    Versao: 1,
}
evento, err := base.CriarEvento(base.InteressePromocao, "principal", payload)
// Trate err, assine o evento, serialize e publique usando a routing key acima.
```

Regras:

- `cadastrar` substitui a lista completa de categorias; exige pelo menos uma categoria.
- `cancelar` com categorias remove somente essas categorias; sem categorias (ou com lista vazia) remove todas.
- Categorias são normalizadas para maiúsculas, ordenadas e deduplicadas. Somente A/B/C são aceitas. E-mail é normalizado para minúsculas e sem espaços nas extremidades; informe somente o endereço, sem nome de exibição.
- `versao` é um inteiro positivo crescente **por e-mail normalizado**. O Gateway deve persistir o contador, publicar as mudanças desse e-mail em ordem e reutilizar a mesma versão/payload nas tentativas da mesma operação. A versão é parte deste contrato de implementação, não um campo exigido pelo enunciado.
- Uma versão anterior é ignorada; uma repetição da versão atual é aceita somente se os dados forem iguais. Isso evita que um cadastro antigo reative uma inscrição cancelada. Cancelamentos parciais dependem das categorias previamente registradas; por isso, preserve a ordem de publicação das alterações.
- Reinscrição exige uma versão maior que a do cancelamento. As versões de inscrições canceladas ficam guardadas para proteger contra mensagens antigas, inclusive após reiniciar.
- Não use o helper de publicação do Principal que recebe `base.OrderPayload` para interesses; ele é específico para pedidos. Crie e assine o envelope com `base.InteressePromocaoPayload` conforme acima.

O Principal/frontend devem implementar os endpoints de cadastro e cancelamento e publicar esses eventos. O MS Promoções não adiciona endpoints HTTP concorrentes ao Gateway. A confirmação HTTP do Gateway indica que a solicitação foi recebida/publicada; a aplicação pelo consumidor é assíncrona.

## Configuração e execução

Na raiz do repositório, com RabbitMQ em `localhost:5672` e as chaves dos serviços disponíveis:

```bash
read -rsp 'Chave da API Resend: ' RESEND_API_KEY
export RESEND_API_KEY
export PROMOCOES_EMAIL_FROM='Loja <ofertas@seu-dominio.com>'
export PROMOCOES_INTERVALO='5m'
go run ./promocoes
```

Use um remetente autorizado na sua conta do Resend. A chave vem exclusivamente do ambiente; não coloque credenciais no Git. O serviço valida a presença da configuração, mas a autorização do remetente e da chave é verificada pela API no envio.

| Variável | Padrão | Finalidade |
| --- | --- | --- |
| `RESEND_API_KEY` | Obrigatória | Credencial da API externa |
| `PROMOCOES_EMAIL_FROM` | Obrigatória | Remetente, aceita `Nome <email>` |
| `PROMOCOES_INTERVALO` | `5m` | Intervalo entre ofertas, duração Go positiva |
| `PROMOCOES_DATA_FILE` | `promocoes/data/promocoes.json` | Inscrições e notificações pendentes |

O intervalo antigo era cinco segundos; o padrão agora é cinco minutos, pois as ofertas geram envios de e-mail. A primeira oferta é gerada após esse intervalo. Pendências são verificadas ao iniciar e a cada 30 segundos. Para demonstração, ajuste o intervalo conscientemente e cadastre somente destinatários de teste autorizados por você.

Somente quem tem interesse ativo na categoria sorteada recebe a oferta. Cada destinatário recebe uma requisição individual; os e-mails dos demais inscritos não são incluídos na mensagem.

## API externa e recuperação de falhas

A integração usa `POST https://api.resend.com/emails`, autenticação Bearer e campos `from`, `to`, `subject` e `text`, conforme a [documentação oficial de envio](https://resend.com/docs/api-reference/emails/send-email). O sucesso exige uma resposta HTTP 2xx com `id` de envio. Isso confirma aceitação pela API, não entrega na caixa de entrada; erros posteriores de entrega devem ser consultados no provedor.

Antes de enviar, o serviço grava a mensagem completa e uma chave única por notificação. Falhas de rede, respostas HTTP de erro (incluindo 429) ou respostas inválidas mantêm a pendência para a próxima rodada. Após sucesso, remove a pendência do arquivo. Alterar o remetente configurado não modifica mensagens já agendadas.

As tentativas reutilizam corpo e header `Idempotency-Key: promocao/<id>`. O Resend conserva essas chaves por 24 horas, segundo sua [documentação de idempotência](https://resend.com/docs/dashboard/emails/idempotency-keys). Esta implementação limita as tentativas a 23 horas desde o agendamento: depois disso, descarta a oferta pendente com registro no log, evitando reenvios fora dessa janela. Uma indisponibilidade prolongada pode, portanto, impedir o envio de uma oferta.

O cancelamento remove as notificações ainda pendentes nas categorias removidas. Uma chamada HTTP já iniciada pode terminar antes que o cancelamento seja aplicado; e-mails já aceitos pelo provedor não podem ser recolhidos pelo serviço.

Acesso ao estado e envio são serializados por mutex, com timeout HTTP de dez segundos. A implementação é adequada à escala do trabalho; não há envio paralelo em massa nem transação distribuída. Use uma instância de Promoções por arquivo JSON. Arquivos inválidos causam erro de inicialização e não são sobrescritos. As chaves das inscrições canceladas e notificações pendentes são persistidas; dados de execução ficam fora do Git.

O evento de interesse só é confirmado após gravar o estado. Eventos inválidos são rejeitados com log; falhas de persistência reenfileiram a mensagem. Inicie Promoções para declarar sua fila antes de publicar os interesses. Reinicie o serviço se a conexão RabbitMQ encerrar.

## Verificação

```bash
go test -race ./...
go vet ./...
go build ./...
```

Os testes usam chaves e arquivos temporários e uma API HTTP local. Cobrem assinatura, validação, filtro por categoria, cadastro, cancelamento parcial/total, versões antigas, duplicatas, concorrência, reinício, falha de gravação, erro da API, repetição com a mesma chave e expiração. Nenhum e-mail real é enviado pelos testes.

Depois da integração do Principal, valide com RabbitMQ e a conta de e-mail configurada:

1. Cadastre interesse em uma categoria e confirme o registro em `promocoes/data/promocoes.json`.
2. Aguarde uma oferta dessa categoria; confira a aceitação no provedor e o recebimento no e-mail.
3. Cancele a categoria e confirme que ofertas posteriores não são enviadas para esse interesse.
4. Reinicie Promoções e confira que o cancelamento continua aplicado.

A implementação e os testes locais não substituem essa validação real, que depende da conta de e-mail e dos endpoints do Principal/frontend.
