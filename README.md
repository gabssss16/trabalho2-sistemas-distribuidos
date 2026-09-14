# trabalho2-sistemas-distribuidos

Backend de um sistema de e-commerce baseado em microsserviços independentes. A comunicação é feita exclusivamente via mensageria (RabbitMQ) utilizando as exchanges `eCommerce` (Direct) e `Promoções` (Topic)[cite: 1]. Todos os eventos trafegados são assinados e validados via criptografia assimétrica RSA[cite: 1].

## 1. Pré-requisitos

* **Go** (versão 1.20 ou superior).
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
