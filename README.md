# trabalho2-sistemas-distribuidos

Backend de um sistema de e-commerce baseado em microsserviços independentes. Os eventos usam RabbitMQ, com as exchanges `eCommerce` (Direct) e `Promoções` (Topic), e são assinados e validados via criptografia assimétrica RSA. Estoque e Pagamento também expõem APIs HTTP para consulta de produtos, checkout e webhook.

Os serviços de **Estoque e Pagamento da Avaliação 3**, incluindo execução, persistência e contrato REST com o mock, estão documentados em [docs/estoque-pagamento.md](docs/estoque-pagamento.md).

## 1. Pré-requisitos

* **Go** na versão indicada em `go.mod` (atualmente 1.27.1).
* **RabbitMQ** rodando na porta padrão (5672).

## 2. Subindo o Servidor RabbitMQ

Você pode iniciar o broker de mensagens de duas formas:

**Opção A: Usando Docker**
No terminal, execute o comando abaixo para baixar e rodar a imagem oficial com o painel de gerenciamento ativo:
`docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`

**Opção B: Instalação Nativa**
Caso prefira rodar direto no notebook sem contêineres, instale e inicie o serviço nativamente pelo terminal:
`sudo apt update`
`sudo apt install rabbitmq-server`
`sudo systemctl enable rabbitmq-server`
`sudo systemctl start rabbitmq-server`
