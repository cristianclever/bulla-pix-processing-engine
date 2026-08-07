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
     * Envia a mensagem para o tópico DLQ caso ocorra erro definitivo de consumo após 2 tentativas.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(pixRequestedDlqTopic, record.partition()));

        // Tenta 2 vezes no consumidor com intervalo de 1s antes de mandar para a DLQ
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}