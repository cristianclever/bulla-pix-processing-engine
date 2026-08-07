package com.fintech.pix.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topics.pix-requested}")
    private String pixRequestedTopic;

    @Value("${app.kafka.topics.pix-requested-retry}")
    private String pixRequestedRetryTopic;

    @Value("${app.kafka.topics.pix-requested-dlq}")
    private String pixRequestedDlqTopic;

    @Bean
    public NewTopic createPixRequestedTopic() {
        return TopicBuilder.name(pixRequestedTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createPixRequestedRetryTopic() {
        return TopicBuilder.name(pixRequestedRetryTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createPixRequestedDlqTopic() {
        return TopicBuilder.name(pixRequestedDlqTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }

    /**
     * Handler de Erros do Consumidor Kafka.
     * 
     * Estratégia:
     * 1. Tenta processar a mensagem novamente 2 vezes com intervalo de 1s
     * 2. Após 2 falhas, envia para o tópico DLQ (Dead Letter Queue) para análise manual
     * 3. Mensagens na DLQ NÃO SÃO reprocessadas automaticamente - requerem intervenção
     * 4. Exceções de negócio não são retentadas (role forward)
     * 
     * IMPORTANTE: O offset é commitado APENAS após sucesso ou envio para DLQ,
     * nunca durante os retries. Isso evita reprocessamento após restart.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(pixRequestedDlqTopic, record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        
        // Exceções de negócio/validação vão direto para DLQ sem retry
        handler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class
        );
        
        return handler;
    }
}