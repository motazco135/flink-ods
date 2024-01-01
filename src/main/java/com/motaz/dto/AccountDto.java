package com.motaz.dto;

import lombok.*;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@ToString
public class AccountDto {
    private Integer id;
    private Integer accountNumber;
    private String accountStatus;
    private String accountBalance;
    private Integer customerId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

}
