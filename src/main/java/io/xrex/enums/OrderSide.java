package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum OrderSide {
    BUY("BUY", "trade.buy", "买入"),
    SELL("SELL", "trade.sell", "卖出");

    @JsonValue
    public final String value;
    public final String languageKey;
    public final String description;

    @JsonCreator
    public static OrderSide fromValue(String value) {
        return Arrays.stream(OrderSide.values())
                .filter(orderSide -> orderSide.value.equals(value))
                .findFirst()
                .orElse(null);
    }
}
