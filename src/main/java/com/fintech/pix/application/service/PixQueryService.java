package com.fintech.pix.application.service;

import com.fintech.pix.application.dto.PixResponseDto;
import com.fintech.pix.domain.exception.TransactionNotFoundException;
import com.fintech.pix.domain.model.PixTransaction;
import com.fintech.pix.domain.model.TransactionStatus;
import com.fintech.pix.domain.repository.PixTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PixQueryService {

    private final PixTransactionRepository transactionRepository;
    private final StringRedisTemplate redisTemplate;

    private static final String REDIS_STATUS_PREFIX = "pix:status:";

    public PixResponseDto getTransactionStatus(String transactionId) {
        String cachedStatus = redisTemplate.opsForValue().get(REDIS_STATUS_PREFIX + transactionId);

        if (cachedStatus != null) {
            log.debug("Cache hit for transactionId: {}", transactionId);
            return PixResponseDto.builder()
                    .transactionId(transactionId)
                    .status(TransactionStatus.valueOf(cachedStatus))
                    .build();
        }

        log.debug("Cache miss for transactionId: {}. Querying database...", transactionId);
        PixTransaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));

        return PixResponseDto.builder()
                .transactionId(transaction.getTransactionId())
                .status(transaction.getStatus())
                .createdAt(transaction.getCreatedAt())
                .build();
    }
}
