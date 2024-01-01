package com.motaz.dto;

import lombok.*;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Builder
public class TransactionDto {
    private Integer id;
    private String  transactionRefNumber;
    private Integer customerId;
    private Integer accountId;
    private Integer transactionAmount;
    private String  transactionType;
    private LocalDateTime createdAt;

}
