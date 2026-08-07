package com.fintech.pix.domain.repository;

import com.fintech.pix.domain.model.PixTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PixTransactionRepository extends JpaRepository<PixTransaction, String> {
}
