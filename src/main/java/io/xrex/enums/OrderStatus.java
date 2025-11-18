package io.xrex.enums;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

import java.util.Arrays;

@AllArgsConstructor
public enum OrderStatus {
    INIT((byte) 0, "trade.order.status.new", "Initial order - not yet accepted"),
    NEW_((byte) 1, "trade.order.status.new", "New order - accepted by the system but not filled"),
    FILLED((byte) 2, "trade.order.status.filled", "Completely filled order - nothing left to process"),
    PART_FILLED((byte) 3, "trade.order.status.part.filled", "Partially filled order - not completely executed"),
    CANCELED((byte) 4, "trade.order.status.canceled", "Cancelled order - no part of it was exceuted"),
    PENDING_CANCEL((byte) 5, "trade.order.status.cancel", "Awaiting cancellation - match engine has not yet acknowledged cancellation"),
    EXPIRED((byte) 6, "trade.order.status.expired", "Expired order - time based order expired before execution"),
    PART_FILLED_CANCELED((byte) 7, "trade.order.status.expired", "Order cancelled before being completely filled - Not stored in the database according to chain up, but I am not sure I believe them"),
    NOT_TRIGGERED((byte) 8, "trade.order.status.not.triggered", "Status for stop order that has not triggered yet");

    @JsonValue
    public final byte value;
    public final String languageKey;
    public final String description;

    @JsonCreator
    public static OrderStatus fromValue(Integer value) {
        return Arrays.stream(OrderStatus.values())
                .filter(orderStatus -> orderStatus.value == value).findFirst().orElse(null);
    }
}
