package com.fintech.pix.domain.repository;

import com.fintech.pix.domain.model.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    @Query(value = "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY created_at ASC FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> findPendingForUpdateSkipLocked(Pageable pageable);
}
