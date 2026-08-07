# Bulla PIX Processing Engine

## Visão Geral

Projeto desenvolvido como resposta ao teste técnico para a posição de **Senior Backend Engineer**.

A solução implementa uma API para processamento de transações PIX utilizando uma arquitetura orientada a eventos, desacoplando o recebimento da requisição do processamento da transação.

## Tecnologias

- Java 21
- Spring Boot 3
- PostgreSQL
- Apache Kafka
- Redis
- Resilience4j
- Micrometer / Prometheus
- Docker

## Pré-requisitos

- Java 21
- Maven
- Docker
- Docker Compose

## Como executar

### Clonar o projeto

```bash
git clone https://github.com/cristianclever/bulla-pix-processing-engine.git
cd bulla-pix-processing-engine
```

### Subir a infraestrutura

```bash
docker compose up -d
```

### Executar a aplicação

```bash
./mvnw spring-boot:run
```

ou

```bash
mvn spring-boot:run
```

## Endpoints

### Criar PIX

`POST /pix`

### Consultar Status

`GET /pix/{transactionId}`

## Estrutura

```
src
├── application
├── domain
├── infrastructure
└── shared
```

## Documentação

- docs/architecture.md
- docs/diagram.md
- docs/decisions.md
