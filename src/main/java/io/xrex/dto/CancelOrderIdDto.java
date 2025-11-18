package io.xrex.dto;

import io.xrex.enums.OrderLeverType;


public record CancelOrderIdDto(Integer chainupId, String pair, OrderLeverType orderType) {
}
