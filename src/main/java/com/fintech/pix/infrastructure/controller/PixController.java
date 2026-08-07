package com.fintech.pix.infrastructure.controller;

import com.fintech.pix.application.dto.PixRequestDto;
import com.fintech.pix.application.dto.PixResponseDto;
import com.fintech.pix.application.service.PixQueryService;
import com.fintech.pix.application.service.PixTransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pix")
@RequiredArgsConstructor
public class PixController {

    private final PixTransactionService transactionService;
    private final PixQueryService queryService;

    @PostMapping
    public ResponseEntity<PixResponseDto> requestPix(@Valid @RequestBody PixRequestDto requestDto) {
        PixResponseDto response = transactionService.createTransaction(requestDto);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<PixResponseDto> getPixStatus(@PathVariable String transactionId) {
        PixResponseDto response = queryService.getTransactionStatus(transactionId);
        return ResponseEntity.ok(response);
    }
}
