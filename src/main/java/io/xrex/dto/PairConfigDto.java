package io.xrex.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PairConfigDto {
    private static final String TABLE_ORDER_PREFIX = "ex_order_";
    private static final String TABLE_TRADE_PREFIX = "ex_trade_";

    private String pair;
    private String base;
    private String quote;
    private Integer baseAccountNormal;
    private Integer baseAccountLock;
    private Integer quoteAccountNormal;
    private Integer quoteAccountLock;
    private Integer sysUid;
    private Integer sysBaseAccount;
    private Integer sysQuoteAccount;

    private Integer baseMmAccountNormal;
    private Integer baseMmAccountLock;
    private Integer quoteMmAccountNormal;
    private Integer quoteMmAccountLock;

    private Integer pricePre;
    private Integer volumePre;

    private BigDecimal minBaseAmount;
    private BigDecimal minQuoteAmount;

    public String getOrderTable() {
        return TABLE_ORDER_PREFIX + this.pair.toLowerCase();
    }

    public String getTradeTable() {
        return TABLE_TRADE_PREFIX + this.pair.toLowerCase();
    }
}
