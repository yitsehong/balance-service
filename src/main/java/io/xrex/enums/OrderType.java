package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum OrderType {
    LIMIT((byte) 1, "限价委托"),
    MARKET((byte) 2, "市价委托"),
    STOP_LIMIT((byte) 3, "限价止盈止损委托");

    @JsonValue
    public final byte value;
    public final String description;

    @JsonCreator
    public static OrderType fromValue(Byte value) {
        return Arrays.stream(OrderType.values())
                .filter(orderType -> orderType.value == value).findFirst().orElse(null);
    }

}
