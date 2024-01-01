package com.motaz.dto;

import lombok.*;

import java.time.LocalDateTime;

@Builder
@ToString
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CustomerAccountDto {

    private Integer customerId;
    private String customerName;
    private String email;
    private String mobile;
    private Integer accountId;
    private String accountNumber;
    private String accountStatus;
    private String accountBalance;
    private LocalDateTime updatedAt;

}
