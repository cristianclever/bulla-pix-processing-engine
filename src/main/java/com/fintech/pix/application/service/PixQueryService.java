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
/**
 * Serviço de consulta para transações PIX.
 *
 * Responsabilidades:
 * - Buscar status de transações usando cache Redis com prefixo 'pix:status:'.
 * - Em caso de cache miss, consultar o repositório persistente e retornar os detalhes.
 *
 * Observações:
 * - Os status armazenados no Redis usam os nomes do enum TransactionStatus.
 */
public class PixQueryService {

    private final PixTransactionRepository transactionRepository;
    private final StringRedisTemplate redisTemplate;

    private static final String REDIS_STATUS_PREFIX = "pix:status:";

    /**
     * Retorna o status da transação PIX identificado por transactionId.
     * <p>
     * Estratégia:
     * - Verifica cache Redis (chave pix:status:{transactionId}) e devolve status em cache se presente (cache hit).
     * - Em cache miss, busca a PixTransaction no repositório e retorna detalhes.
     *
     * @param transactionId identificador da transação
     * @return PixResponseDto contendo transactionId, status e createdAt quando disponível
     * @throws TransactionNotFoundException se a transação não for encontrada no repositório
     */
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
