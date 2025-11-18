package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum TradeType {
    MAKER("maker"),
    TAKER("taker");

    @JsonValue
    public final String value;

    @JsonCreator
    public static TradeType fromValue(String value) {
        return Arrays.stream(TradeType.values())
                .filter(tradeType -> tradeType.value.equalsIgnoreCase(value)).findFirst().orElse(null);
    }
}
