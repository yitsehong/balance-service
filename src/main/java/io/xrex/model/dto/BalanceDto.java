package io.xrex.model.dto;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * Represents a user's balance in the in-memory store.
 */
public class BalanceDto {

    private final AccountIdDto accountIdDto;
    @Setter
    @Getter
    private String coinSymbol;
    @Setter
    @Getter
    private BigDecimal amount;
    @Setter
    @Getter
    private String accountTag;

    public BalanceDto(AccountIdDto accountIdDto, String coinSymbol, BigDecimal initialAmount, String accountTag) {
        this.accountIdDto = accountIdDto;
        this.coinSymbol = coinSymbol;
        this.amount = initialAmount;
        this.accountTag = accountTag;
    }

    public AccountIdDto getAccountId() {
        return accountIdDto;
    }

}
