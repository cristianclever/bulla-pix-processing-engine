# Diagrama da arquitetura proposta

```mermaid
flowchart TD
    subgraph entry[Camada de entrada]
        user[Cliente / Integrador]
        api[API Spring Boot]
    end

    subgraph data[Persistencia e consistencia]
        db[(PostgreSQL)]
        outbox[(Tabela Outbox)]
        cache[(Redis)]
    end

    subgraph messaging[Mensageria e entrega]
        publisher[Outbox Publisher]
        kafka[(Apache Kafka)]
        dlq[(Dead Letter Queue)]
    end

    subgraph processing[Processamento assinc]
        consumer[PIX Consumer]
        partner[Instituicao Financeira]
    end

    subgraph ops[Observabilidade]
        metrics[Micrometer / Prometheus]
    end

    user -->|POST /pix| api
    user -->|GET /pix/{id}| api

    api -->|Persiste transacao| db
    api -->|Registra evento| outbox
    api -->|Consulta status| db

    outbox --> publisher
    publisher -->|Publica mensagem| kafka

    kafka -->|Consome evento| consumer
    consumer -->|Atualiza status| db
    consumer -->|Leitura cache| cache
    consumer -->|Falha persistida| dlq
    consumer -->|Processa PIX| partner

    api --> metrics
    consumer --> metrics
```