package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.enums.*;
import io.xrex.persistence.entity.ExOrderEntity;
import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TradeEventDto {

    private String pair;
    private Long orderId;
    private Integer chainupId;
    private OrderSide orderSide;
    private ExTradeDto trade;

    @JsonIgnore
    public ExOrderEntity toMMExOrderEntity(Integer mmChainupId) {
        ExTradeDto exTrade = this.getTrade();
        BigDecimal dealMoney = exTrade.getVolume().multiply(exTrade.getPrice());
        return ExOrderEntity.builder()
                .userId(mmChainupId)
                .side(OrderSide.BUY == this.getOrderSide() ? OrderSide.SELL : OrderSide.BUY)
                .price(exTrade.getPrice())
                .volume(exTrade.getVolume())
                .feeDeductType(FeeDeductType.INNER)
                .feeRateMaker(0d)
                .feeRateTaker(0d)
                .fee(BigDecimal.ZERO)
                .feeCoinRate(0d)
                .dealVolume(exTrade.getVolume())
                .dealMoney(dealMoney)
                .avgPrice(exTrade.getPrice())
                .lockedAmount(OrderSide.BUY == this.getOrderSide() ? exTrade.getVolume() : dealMoney)
                .status(OrderStatus.FILLED)
                .type(OrderType.LIMIT)
                .source(OrderSourceType.ROBOT)
                .orderType(OrderLeverType.MARKET_MAKING_ORDER)
                .build();
    }
}
