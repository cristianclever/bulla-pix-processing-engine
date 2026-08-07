package com.fintech.pix.infrastructure.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.application.dto.PartnerIntegrationEvent;
import com.fintech.pix.domain.model.PixTransaction;
import com.fintech.pix.domain.model.TransactionStatus;
import com.fintech.pix.domain.repository.PixTransactionRepository;
import com.fintech.pix.infrastructure.client.PartnerBankClientMock;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class PixTransactionConsumer {

    private final PartnerBankClientMock partnerBankClient;
    private final PixTransactionRepository transactionRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String REDIS_STATUS_PREFIX = "pix:status:";
    private static final String REDIS_LOCK_PREFIX = "pix:lock:";



    @KafkaListener(topics = "${app.kafka.topics.pix-requested}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    @SneakyThrows
    public void consumeTransaction(String message, Acknowledgment ack) {
        PartnerIntegrationEvent event = objectMapper.readValue(message, PartnerIntegrationEvent.class);
        String txId = event.getTransactionId();

        log.info("Received Kafka event for transactionId: {}", txId);

        // 1. Idempotência via Redis Lock
        Boolean lockAcquired = redisTemplate.opsForValue().setIfAbsent(
                REDIS_LOCK_PREFIX + txId,
                "LOCKED",
                Duration.ofSeconds(30)
        );

        if (Boolean.FALSE.equals(lockAcquired)) {
            log.warn("TransactionId {} is already being processed. Skipping.", txId);
            ack.acknowledge(); // Comita offset para liberar a mensagem duplicada
            return;
        }

        try {
            PixTransaction transaction = transactionRepository.findById(txId).orElse(null);
            if (transaction != null && transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.info("Transaction {} already has final status {}. Skipping.", txId, transaction.getStatus());
                ack.acknowledge();
                return;
            }

            // 2. Chamada protegida ao Mock via Resilience4j
            boolean partnerApproved = partnerBankClient.processTransaction(event).get();

            TransactionStatus finalStatus = partnerApproved ? TransactionStatus.SUCCESS : TransactionStatus.FAILED;

            if (transaction != null) {
                transaction.setStatus(finalStatus);
                transactionRepository.save(transaction);
            }

            redisTemplate.opsForValue().set(
                    REDIS_STATUS_PREFIX + txId,
                    finalStatus.name(),
                    Duration.ofHours(24)
            );

            log.info("TransactionId {} updated to status {}", txId, finalStatus);

            // 3. CONFIRMAÇÃO MANUAL DE PROCESSAMENTO (ACK)
            ack.acknowledge();

        } catch (Exception e) {
            markTransactionAsFailed(txId);
            redisTemplate.opsForValue().set(
                    REDIS_STATUS_PREFIX + txId,
                    TransactionStatus.FAILED.name(),
                    Duration.ofHours(24)
            );
            log.error("Error processing transaction {}. Marked as FAILED and rethrowing for DLQ.", txId, e);
            throw e;
        } finally {
            redisTemplate.delete(REDIS_LOCK_PREFIX + txId);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markTransactionAsFailed(String txId) {
        PixTransaction transaction = transactionRepository.findById(txId).orElse(null);
        if (transaction == null) {
            log.warn("Transaction {} not found in database; skipping DB status update.", txId);
            return;
        }

        transaction.setStatus(TransactionStatus.FAILED);
        transactionRepository.save(transaction);
        log.info("TransactionId {} marked as FAILED in Postgres", txId);
    }












}