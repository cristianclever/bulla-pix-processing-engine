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
    @SneakyThrows
    public void consumeTransaction(String message, Acknowledgment ack) {

        PartnerIntegrationEvent event = objectMapper.readValue(message, PartnerIntegrationEvent.class);
        String txId = event.getTransactionId();

        log.info("Processing Kafka event for transactionId: {}", txId);

        // Lock de Idempotência
        Boolean lockAcquired = redisTemplate.opsForValue().setIfAbsent(REDIS_LOCK_PREFIX + txId, "LOCKED", Duration.ofSeconds(30));

        if (Boolean.FALSE.equals(lockAcquired)) {
            log.warn("TransactionId {} locked by another worker. Skipping.", txId);
            ack.acknowledge();
            return;
        }

        try {
            PixTransaction transaction = transactionRepository.findById(txId).orElse(null);

            if (transaction != null && transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.info("Transaction {} already finalized with status {}. Skipping.", txId, transaction.getStatus());
                ack.acknowledge();
                return;
            }

            // 1. Chamada ao Parceiro
            boolean partnerApproved = partnerBankClient.processTransaction(event).get();

            // 2. Tratamento do Retorno (true ou false)
            TransactionStatus finalStatus = partnerApproved ? TransactionStatus.SUCCESS : TransactionStatus.FAILED;
            updateStatus(transaction, txId, finalStatus);

            // 3. Confirmar mensagem no Kafka APENAS APÓS sucesso garantido
            ack.acknowledge();
            log.info("TransactionId {} successfully consumed and offset committed.", txId);

        } catch (Exception ex) {
            // 4. Tratamento das Exceções (Timeout, Resilience4j, Erro de Infra)
            log.error("Integration failure for transactionId: {}. Will retry via DefaultErrorHandler.", txId, ex);

            // Marcar como FAILED de forma explícita (fora de @Transactional para garantir persistência)
            try {
                PixTransaction transaction = transactionRepository.findById(txId).orElse(null);
                updateStatus(transaction, txId, TransactionStatus.FAILED);
                log.info("TransactionId {} status updated to FAILED in database.", txId);
            } catch (Exception statusEx) {
                log.error("Failed to update transaction status for {}", txId, statusEx);
            }

            // NÃO fazer ack.acknowledge(). DefaultErrorHandler tentará 2 vezes antes de enviar para DLQ
            throw new RuntimeException("Partner integration exception for transaction: " + txId, ex);

        } finally {
            // Apenas limpar o lock. NÃO fazer acknowledge aqui
            redisTemplate.delete(REDIS_LOCK_PREFIX + txId);
        }
    }




    private void updateStatus(PixTransaction transaction, String txId, TransactionStatus status) {

        if (transaction != null) {
            transaction.setStatus(status);
            transactionRepository.save(transaction);
        }
        redisTemplate.opsForValue().set(
                REDIS_STATUS_PREFIX + txId,
                status.name(),
                Duration.ofHours(24)
        );
        log.info("TransactionId {} updated to status {}", txId, status);
    }




}
