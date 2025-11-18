package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum OrderLeverType {
    NORMAL_ORDER((byte) 1, "正常订单"),
    LEVER_ORDER((byte) 2, "杠杆订单"),
    MARGIN_ORDER((byte) 3, "Margin Order"),
    GRID_ORDER((byte) 4, "Grid Bot Order"),
    GRID_MARGIN_ORDER((byte) 5, "Grid Margin Order"),
    MARKET_MAKING_ORDER((byte) 6, "Market making Order"),
    CONVERT_ORDER((byte) 7, "Convert Order"),
    UNKNOWN_ORDER((byte) 99, "Unknown Order");

    @JsonValue
    public final byte value;
    public final String description;

    @JsonCreator
    public static OrderLeverType fromValue(Byte value) {
        return Arrays.stream(OrderLeverType.values())
                .filter(orderLeverType -> orderLeverType.value == value).findFirst().orElse(null);
    }
}
