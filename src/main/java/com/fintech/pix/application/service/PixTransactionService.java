package com.fintech.pix.application.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.application.dto.PartnerIntegrationEvent;
import com.fintech.pix.application.dto.PixRequestDto;
import com.fintech.pix.application.dto.PixResponseDto;
import com.fintech.pix.domain.exception.TransactionAlreadyExistsException;
import com.fintech.pix.domain.model.*;
import com.fintech.pix.domain.repository.OutboxRepository;
import com.fintech.pix.domain.repository.PixTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class PixTransactionService {

    private final PixTransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String REDIS_STATUS_PREFIX = "pix:status:";

    @Transactional
    @SneakyThrows
    public PixResponseDto createTransaction(PixRequestDto dto) {
        log.info("Processing creation request for transactionId: {}", dto.getTransactionId());

        final String cacheKey = REDIS_STATUS_PREFIX + dto.getTransactionId();

        // 1. Trava atômica / Registro de Idempotência no Redis (Fail-Fast em <1ms)
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(
                cacheKey,
                TransactionStatus.PROCESSING.name(),
                Duration.ofHours(24)
        );

        // Se a chave já existir, lança exceção para retornar HTTP 409 Conflict imediatamente
        if (Boolean.FALSE.equals(isNew)) {
            log.warn("TransactionId {} is already registered or being processed", dto.getTransactionId());
                throw new TransactionAlreadyExistsException(dto.getTransactionId());
        }

        try {
            // 2. Criação da transação principal
            PixTransaction transaction = PixTransaction.builder()
                    .transactionId(dto.getTransactionId())
                    .amount(dto.getAmount())
                    .pixKey(dto.getPixKey())
                    .description(dto.getDescription())
                    .status(TransactionStatus.PROCESSING)
                    .build();

            transactionRepository.save(transaction);

            // 3. Criação do evento no Outbox (Transactional Outbox Pattern)
            PartnerIntegrationEvent event = PartnerIntegrationEvent.builder()
                    .transactionId(dto.getTransactionId())
                    .amount(dto.getAmount())
                    .pixKey(dto.getPixKey())
                    .description(dto.getDescription())
                    .build();

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("PixTransaction")
                    .aggregateId(dto.getTransactionId())
                    .eventType("PIX_REQUESTED")
                    .payload(objectMapper.writeValueAsString(event))
                    .status(OutboxStatus.PENDING)
                    .build();

            outboxRepository.save(outboxEvent);

            // 4. Retorno imediato HTTP 202 Accepted para o cliente
            return PixResponseDto.builder()
                    .transactionId(transaction.getTransactionId())
                    .status(TransactionStatus.PROCESSING)
                    .createdAt(OffsetDateTime.now())
                    .build();

        } catch (Exception e) {
            // Caso ocorra qualquer falha durante a gravação no Postgres,
            // remove a chave do Redis para não deixar uma trava "fantasma"
            redisTemplate.delete(cacheKey);
            log.error("Error persisting transaction {}. Evicted Redis key.", dto.getTransactionId(), e);
            throw e;
        }
    }
}
