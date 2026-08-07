package com.fintech.pix.infrastructure.client;

import com.fintech.pix.application.dto.PartnerIntegrationEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class PartnerBankClientMock {

    private final Random random = new Random();

    /**
     * O Resilience4j TimeLimiter exige que métodos assíncronos/protegidos por timeout
     * retornem CompletableFuture ou CompletionStage em cenários não-reativos.
     */
    @CircuitBreaker(name = "partnerBank")
    @TimeLimiter(name = "partnerBank")
    @Retry(name = "partnerBank")
    public CompletableFuture<Boolean> processTransaction(PartnerIntegrationEvent event) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("## Calling Partner Bank API for transactionId: {}...", event.getTransactionId());

            //Forcando eventos
            if (event.getTransactionId().contains("TIMEOUT")) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }else if (event.getTransactionId().contains("FAIL")) {
                throw new IllegalStateException("Integration failure with partner bank");
            } else if (event.getTransactionId().contains("REJECT")) {
                return false;
            }



            try {
                // Simula a latência base do parceiro (~2s)
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Execution interrupted", e);
            }


            log.info("Partner Bank APPROVED transactionId: {}", event.getTransactionId());
            return true;
        });
    }

}