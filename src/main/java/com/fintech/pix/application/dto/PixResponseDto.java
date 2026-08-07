package com.fintech.pix.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fintech.pix.domain.model.TransactionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PixResponseDto {

    private String transactionId;
    private TransactionStatus status;
    private OffsetDateTime createdAt;
}
