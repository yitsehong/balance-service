package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum StopPriceDirection {
    GTE((byte) 1, "大于或等于"),
    LTE((byte) 2, "小于或等于");

    @JsonValue
    public final byte value;
    public final String description;

    @JsonCreator
    public static StopPriceDirection fromValue(Byte value) {
        return Arrays.stream(StopPriceDirection.values())
                .filter(stopPriceDirection -> stopPriceDirection.value == value).findFirst().orElse(null);
    }
}
