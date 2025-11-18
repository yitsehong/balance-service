package io.xrex.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum FeeDeductType {

    INNER(1, "fee deduct include"),
    OUTER(2, "fee deduct exclude");

    @JsonValue
    public final int value;
    public final String description;

    @JsonCreator
    public static FeeDeductType fromValue(Integer value) {
        return Arrays.stream(FeeDeductType.values())
                .filter(feeDeductType -> feeDeductType.value == value).findFirst().orElse(null);
    }
}
