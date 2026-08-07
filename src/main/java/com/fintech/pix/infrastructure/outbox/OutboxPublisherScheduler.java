package com.fintech.pix.infrastructure.outbox;

import com.fintech.pix.domain.model.OutboxEvent;
import com.fintech.pix.domain.model.OutboxStatus;
import com.fintech.pix.domain.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherScheduler {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.pix-requested}")
    private String pixRequestedTopic;

    @Value("${outbox.publisher.batch-size:50}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:5000}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxRepository.findPendingForUpdateSkipLocked(PageRequest.of(0, batchSize));

        if (events.isEmpty()) {
            return;
        }

        log.info("Found {} pending outbox events to publish", events.size());

        for (OutboxEvent event : events) {
            processSingleEvent(event);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processSingleEvent(OutboxEvent event) {
        try {
            //Envia ao kafka - 5 tentativas
            kafkaTemplate.send(pixRequestedTopic, event.getAggregateId(), event.getPayload()).get();
            event.setStatus(OutboxStatus.PUBLISHED);
            log.debug("Successfully published outbox event id: {} for tx: {}", event.getId(), event.getAggregateId());
        } catch (Exception e) {
            log.error("Failed to publish outbox event id: {}. Error: {}", event.getId(), e.getMessage());
            event.setRetryCount(event.getRetryCount() + 1);
            if (event.getRetryCount() >= 5) {
                event.setStatus(OutboxStatus.FAILED);
            }
        }
        outboxRepository.save(event);

    }


}
