# PIX Processing Engine

Uma solução de alta vazão, baixa latência e alta disponibilidade projetada para o processamento assíncrono e resiliente de transações PIX.

---

## 1. Contexto e Identificação dos Gargalos

A arquitetura legada apresentava um processamento **estritamente síncrono e bloqueante**, gerando um tempo de resposta P95 de ~4 segundos por transação.

### Gargalos Identificados no Fluxo Anterior:
1. **Acoplamento Síncrono End-to-End:** A requisição HTTP do cliente ficava retida enquanto a transação passava por múltiplos saltos de rede/software (API → Stored Procedure → Legado → Core → Parceiro).
2. **Gargalo no Banco de Dados (Stored Procedures):** Execução de lógica de negócio e concorrência de locks diretamente no banco relacional, consumindo CPU/Memória do componente mais caro para escalar horizontalmente.
3. **Latência Inevitável do Parceiro (~2s):** A integração com a instituição financeira parceira consome metade do P95 atual e possui falhas temporárias. Prender a thread HTTP durante esses 2 segundos causava o esgotamento rápido do pool de conexões sob alto volume.

---

## 2. Arquitetura Proposta

Para suportar o crescimento da operação e eliminar a latência no cliente, o processamento foi transformado em um **modelo assíncrono orientado a eventos** combinando o padrão **CQRS** com o **Transactional Outbox Pattern**.

### Diagrama da Arquitetura

```mermaid
flowchart TD
    subgraph Cliente["Cliente / App"]
        C1["POST /pix (Solicitação)"]
        C2["GET /pix/{id} (Consulta Status)"]
    end

    subgraph API["API PIX (Spring Boot / Java 21 + Virtual Threads)"]
        CTRL["PixController"]
        SVC_WRITE["PixTransactionService"]
        SVC_READ["PixQueryService"]
    end

    subgraph Storage["Camada de Persistência & Cache"]
        PG[(PostgreSQL 16)]
        outbox_tbl["Tabela: outbox"]
        tx_tbl["Tabela: pix_transactions"]
        REDIS[(Redis 7 - Cache & Idempotency)]
    end

    subgraph Messaging["Mensageria Assíncrona"]
        PUB["OutboxPublisherScheduler<br/>(FOR UPDATE SKIP LOCKED)"]
        KAFKA{{Apache Kafka Broker}}
        TOPIC_REQ["Topic: pix-transactions-requested"]
        TOPIC_RETRY["Topic: pix-transactions-requested-retry"]
        TOPIC_DLQ["Topic: pix-transactions-requested-dlq"]
    end

    subgraph Processing["Worker / Consumidor & Integração"]
        CONSUMER["PixTransactionConsumer<br/>(Lock Distribuído via Redis)"]
        RESILIENCE["Resilience4j<br/>(CircuitBreaker + TimeLimiter + Retry)"]
        MOCK["Instituição Financeira Parceira<br/>(Latência ~2s + Falhas Temporárias)"]
    end

    %% Fluxo de Escrita (POST)
    C1 -->|1. HTTP Payload| CTRL
    CTRL -->|2. Validação & Ingestão| SVC_WRITE
    SVC_WRITE -->|3. Idempotência / Trava Fail-Fast| REDIS
    SVC_WRITE -->|4. Transação Local ACID| PG
    PG --- tx_tbl
    PG --- outbox_tbl
    SVC_WRITE -->|5. HTTP 202 Accepted| C1

    %% Fluxo de Outbox para Kafka
    PUB -->|6. Polling em Batch| outbox_tbl
    PUB -->|7. Publica Evento| TOPIC_REQ
    KAFKA --- TOPIC_REQ
    KAFKA --- TOPIC_RETRY
    KAFKA --- TOPIC_DLQ

    %% Fluxo de Consumo
    TOPIC_REQ -->|8. Evento| CONSUMER
    CONSUMER -->|9. Trava de Execução SETNX| REDIS
    CONSUMER -->|10. Executa Chamada Protegida| RESILIENCE
    RESILIENCE -->|11. Integração HTTP (~2s)| MOCK
    CONSUMER -->|12. Atualiza Status Consolidado| PG
    CONSUMER -->|13. Evict / Update Status Cache| REDIS

    %% Fluxo de Leitura (GET - CQRS)
    C2 -->|1. HTTP Consultation| CTRL
    CTRL --> SVC_READ
    SVC_READ -->|2. Cache First (<2ms)| REDIS
    SVC_READ -.->|3. Fallback (Cache Miss)| PG
    SVC_READ -->|4. Status HTTP 200| C2
```

---

## 3. Decisões Arquiteturais e Principais Tecnologias

* **Java 21 + Virtual Threads (Project Loom):** Habilita alto *throughput* de I/O na comunicação com o parceiro sem a sobrecarga de memória das Threads tradicionais do sistema operacional.
* **Transactional Outbox Pattern com `FOR UPDATE SKIP LOCKED`:** Evita o problema do *dual-write* (escrever no banco e falhar no Kafka). A query com `SKIP LOCKED` permite que múltiplas instâncias da API façam o *polling* do outbox simultaneamente sem contenção de locks ou *deadlocks*.
* **Apache Kafka (6 Partições):** Garante ordenamento de mensagens por chave, desacoplamento e capacidade de absorber picos de tráfego (*backpressure* natural).
* **CQRS + Redis Cache:** 100% das consultas de status (`GET /pix/{id}`) são servidas em memória RAM pelo Redis em **<2ms**, isolando o PostgreSQL do tráfego massivo de *polling* de clientes.
* **Resilience4j (`TimeLimiter` + `CircuitBreaker` + `Retry`):**
  * `TimeLimiter` (3.5s): Corta execuções que excedam o limite para não pendurar workers.
  * `CircuitBreaker`: Entra em modo *OPEN* em falhas sequenciais do parceiro, promovendo *fail-fast* imediato para proteger a infraestrutura.

---

## 4. Trade-offs Considerados

| Decisão | Vantagem | Trade-off / Ponto de Atenção |
| :--- | :--- | :--- |
| **Processamento Assíncrono** | Resposta HTTP imediata ao cliente (<15ms) e alta resiliência. | O cliente precisa consultar o status via `GET /pix/{id}` ou via Webhook/WebSocket. |
| **Polling Outbox (`SKIP LOCKED`)** | Simplicidade de implementação sem necessidade de componentes CDC externos (ex: Debezium). | Gera um *overhead* mínimo de queries periódicas (`SELECT`) no banco de dados. |
| **Redis para Idempotência** | Validação ultra-rápida na camada de borda, barrando requisições duplicadas. | Dependência de um cluster Redis com alta disponibilidade (Sentinel/Cluster). |

---

## 5. Como Executar o Projeto

### Pré-requisitos
* Docker e Docker Compose instalados.
* Java 21 e Maven (caso deseje rodar a aplicação fora do Docker).

### Passo a Passo

1. **Subir a Infraestrutura (PostgreSQL, Redis e Kafka):**
   ```bash
   docker compose up -d
   ```
   *O PostgreSQL será inicializado automaticamente criando os schemas e índices otimizados através do arquivo `docker/init.sql`.*

2. **Executar a Aplicação Spring Boot:**
   ```bash
   ./mvnw spring-boot:run
   ```

---

## 6. Testando os Endpoints (Exemplos cURL)

### 1. Criar Transação PIX (`POST /pix`)
```bash
curl -X POST http://localhost:8080/pix \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "tx-100001",
    "amount": 150.75,
    "pixKey": "cliente@email.com",
    "description": "Pagamento de fatura"
  }'
```
* **Resposta Esperada:** HTTP **`202 Accepted`** em <15ms.

### 2. Consultar Status (`GET /pix/{id}`)
```bash
curl -X GET http://localhost:8080/pix/tx-100001
```
* **Resposta Esperada:** HTTP **`200 OK`** com status `PROCESSING` (nos primeiros ~2s) e atualizando para `SUCCESS` ou `FAILED` após a conclusão da integração.

---

## 7. Observabilidade e Monitoramento Futuro

Para um ambiente produtivo sob altíssimo volume, a solução está preparada para integração com:

* **Métricas (Spring Boot Actuator + Prometheus + Grafana):**
  * Métricas de tamanho da fila/lag do Kafka por grupo consumidor.
  * Métrica do tempo de execução da chamada ao parceiro via Resilience4j.
  * Contadores de status da tabela Outbox (`PENDING`, `PUBLISHED`, `FAILED`).
* **Distributed Tracing (Micrometer Tracing + Zipkin/Jaeger):**
  * Propagação do `traceId` do cabeçalho HTTP até o evento do Kafka e logs do consumidor, permitindo rastrear o ciclo de vida completo de cada `transactionId`.
* **Dead Letter Queue (DLQ Management):**
  * Tópico `pix-transactions-requested-dlq` configurado para isolar transações com falhas definitivas para análise e reprocessamento posterior.
