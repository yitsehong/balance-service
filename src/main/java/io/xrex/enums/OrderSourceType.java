package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum OrderSourceType {
    WEB((byte) 1,"WEB"),
    APP((byte) 2,"APP"),
    API((byte) 3,"API"),
    H5((byte) 4,"H5"),
    ROBOT((byte) 5,"ROBOT"),
    CM((byte) 10,"CM"),
    MYEXCHANGE((byte) 11, "MYEXCHANGE"),
    GRID_BOT((byte) 12, "Grid Bot"),
    PARTNER_API((byte) 13, "Partner API"),
    A2Z((byte) 14, "A2Z"),
    MARKET_MAKING((byte) 15,"MARKET_MAKING"), // 以後好區分機器人(ROBOT) 跟 外部造市商
    ;

    @JsonValue
    public final byte value;
    public final String description;

    @JsonCreator
    public static OrderSourceType fromValue(Byte value) {
        return Arrays.stream(OrderSourceType.values())
                .filter(orderSourceType -> orderSourceType.value == value).findFirst().orElse(null);
    }
}
