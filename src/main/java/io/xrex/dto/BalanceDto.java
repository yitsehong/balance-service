package io.xrex.dto;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * Represents a user's balance in the in-memory store.
 */
@lombok.Data
@lombok.NoArgsConstructor
public class BalanceDto {

    private AccountIdDto accountIdDto;
    private BigDecimal amount;

    public BalanceDto(AccountIdDto accountIdDto, BigDecimal initialAmount) {
        this.accountIdDto = accountIdDto;
        this.amount = initialAmount;
    }

    public AccountIdDto getAccountId() {
        return accountIdDto;
    }

    @Override
    public String toString() {
        return "BalanceDto{" +
                "accountIdDto=" + accountIdDto +
                ", amount=" + amount +
                '}';
    }
}
