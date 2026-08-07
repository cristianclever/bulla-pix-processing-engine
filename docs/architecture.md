# Architecture Document

# Bulla PIX Processing Engine

## 1. Objetivo

Este documento descreve a arquitetura da solução desenvolvida para o processamento de transações PIX, apresentando as decisões arquiteturais, justificativas técnicas, trade-offs e mecanismos de resiliência adotados.

A solução foi construída para atender ao cenário proposto no teste técnico, cujo principal desafio consiste em reduzir o impacto da latência da instituição financeira e permitir crescimento horizontal da plataforma.

---

# 2. Problema

A arquitetura original realizava todo o processamento de forma síncrona.

Consequências:

- Alta latência percebida pelo cliente;
- Baixo throughput;
- Forte acoplamento entre API e integração externa;
- Baixa resiliência a falhas temporárias.

---

# 3. Objetivos Arquiteturais

- Desacoplar API do processamento.
- Permitir escalabilidade horizontal.
- Garantir confiabilidade na publicação de eventos.
- Evitar perda de mensagens.
- Tratar falhas temporárias automaticamente.
- Facilitar observabilidade.

---

# 4. Arquitetura Proposta

```text
Cliente
   |
POST /pix
   |
Spring Boot API
   |
PostgreSQL
   |
Outbox Pattern
   |
Outbox Publisher
   |
Apache Kafka
   |
PIX Consumer
   |
+----------------------------+
| Retry / Circuit Breaker    |
| TimeLimiter                |
+----------------------------+
   |
Instituição Financeira
   |
Atualização do Status
```

A API retorna rapidamente ao cliente após persistir a transação e registrar um evento na Outbox. O processamento é realizado de forma assíncrona por consumidores Kafka.

---

# 5. Componentes

## API

Responsável por validação, persistência e consulta de status.

## PostgreSQL

Armazena transações e eventos da Outbox de forma transacional.

## Outbox Pattern

Garante consistência entre banco de dados e publicação no Kafka, evitando perda de eventos em caso de falhas.

## Kafka

Responsável pelo desacoplamento entre recepção da requisição e processamento.

Benefícios:

- Escalabilidade horizontal;
- Balanceamento entre consumidores;
- Alta vazão.

## Consumer

Processa mensagens utilizando ACK manual, confirmando o consumo somente após sucesso do processamento.

## Redis

Utilizado como apoio para otimização de acesso a dados e redução de carga sobre o banco de dados.

---

# 6. Resiliência

## Retry

Falhas transitórias são tratadas automaticamente utilizando Resilience4j.

## Circuit Breaker

Evita sobrecarga da integração externa quando esta apresenta indisponibilidade.

## TimeLimiter

Impede chamadas bloqueadas por tempo excessivo.

## Dead Letter Queue (DLQ)

Mensagens que excedem o limite configurado de tentativas são encaminhadas para uma Dead Letter Queue.

Isso permite:

- preservar a mensagem;
- evitar bloqueio do consumidor;
- possibilitar análise e reprocessamento posterior.

---

# 7. Idempotência

A transação é identificada por um identificador único.

Reprocessamentos não geram efeitos colaterais, permitindo retries seguros.

---

# 8. Escalabilidade

A arquitetura foi projetada para escalabilidade horizontal.

Novas instâncias podem ser adicionadas sem alteração no código.

O Kafka distribui automaticamente as mensagens entre os consumidores do mesmo Consumer Group.

---

# 9. Observabilidade

A solução contempla:

- Logs estruturados;
- Micrometer;
- Prometheus;
- Tracing distribuído;
- Monitoramento do Kafka;
- Métricas de processamento.

---

# 10. Trade-offs

## Benefícios

- Alta disponibilidade;
- Baixa latência para o cliente;
- Desacoplamento entre API e processamento;
- Escalabilidade horizontal;
- Resiliência.

## Custos

- Maior complexidade arquitetural;
- Mais componentes de infraestrutura;
- Necessidade de monitoramento contínuo;
- Maior esforço operacional.

---

# 11. Evoluções Futuras

- Kubernetes;
- Schema Registry;
- Dashboards Grafana;
- Rate Limiting;
- OAuth2/JWT;
- CI/CD;

---

# 12. Conclusão

A arquitetura proposta atende aos requisitos do desafio ao desacoplar o processamento da requisição HTTP, reduzir o impacto da latência da instituição financeira e oferecer uma plataforma preparada para crescimento. O uso combinado de Event-Driven Architecture, Outbox Pattern, Kafka, Resilience4j, Redis e observabilidade fornece uma base robusta para ambientes de alta demanda.
