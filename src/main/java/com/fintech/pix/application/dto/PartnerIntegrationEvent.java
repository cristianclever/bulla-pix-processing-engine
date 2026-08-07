package com.fintech.pix.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartnerIntegrationEvent {

    private String transactionId;
    private BigDecimal amount;
    private String pixKey;
    private String description;
}
