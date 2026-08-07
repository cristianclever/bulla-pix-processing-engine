# Diagrama da arquitetura proposta

```mermaid
flowchart LR
    Client[Cliente / Integrador]

    subgraph Entrada[Camada de entrada]
        API[Spring Boot API]
    end

    subgraph Persistencia[Persistência e consistência]
        DB[(PostgreSQL)]
        Outbox[(Tabela de Outbox)]
        Cache[(Redis)]
    end

    subgraph Mensageria[Mensageria e entrega]
        Publisher[Outbox Publisher]
        Kafka[(Apache Kafka)]
        DLQ[(Dead Letter Queue)]
    end

    subgraph Processamento[Processamento assíncrono]
        Consumer[PIX Consumer]
        Resilience[Resilience4j<br/>Retry / Circuit Breaker / TimeLimiter]
        Partner[Instituição Financeira]
    end

    subgraph Observabilidade[Observabilidade]
        Metrics[Micrometer / Prometheus]
        Logs[Logs estruturados]
        Tracing[Tracing distribuído]
    end

    Client -->|POST /pix| API
    Client -->|GET /pix/{id}| API

    API -->|Persiste transação| DB
    API -->|Registra evento na outbox| Outbox
    API -->|Consulta status| DB

    Outbox --> Publisher
    Publisher -->|Publica evento| Kafka

    Kafka -->|Consome mensagem| Consumer
    Consumer -->|Aplica políticas de resiliência| Resilience
    Resilience -->|Chamada externa| Partner
    Consumer -->|Atualiza status| DB
    Consumer -->|Cache de leitura| Cache
    Consumer -->|Falha persistida| DLQ

    API --> Metrics
    Consumer --> Metrics
    API --> Logs
    Consumer --> Logs
    API --> Tracing
    Consumer --> Tracing
```