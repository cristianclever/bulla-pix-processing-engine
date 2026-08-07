```mermaid
flowchart LR

    Client[Cliente]

    API[PIX API]

    DB[(PostgreSQL)]

    Outbox[(Outbox)]

    Publisher[Outbox Publisher]

    Kafka[(Apache Kafka)]

    Consumer[PIX Consumer]

    Partner[Instituição Financeira]

    Client -->|POST /pix| API

    API -->|Persistência| DB

    API -->|Grava Evento| Outbox

    Outbox --> Publisher

    Publisher -->|Publica Evento| Kafka

    Kafka -->|Consome Evento| Consumer

    Consumer -->|Processa PIX| Partner

    Consumer -->|Atualiza Status| DB

    Client -->|GET /pix/{id}| API

    API --> DB
```