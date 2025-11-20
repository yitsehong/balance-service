package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.xrex.enums.*;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.util.BigDecimalToStringSerializer;
import io.xrex.util.XrexConstant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExTradeDto {
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal price;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal volume;
    private Long bidId;
    private Long askId;
    private String trendSide;
    private Integer bidUserId;
    private Integer askUserId;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal buyFee;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal sellFee;
    private String buyFeeCoin;
    private String sellFeeCoin;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime ctime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime mtime;
    private Byte buyType;
    private Byte sellType;

    private String tradeNonce;

    public ExTradeEntity toEntity() {
        return ExTradeEntity.builder()
                .price(this.price)
                .volume(this.volume)
                .bidId(this.bidId)
                .askId(this.askId)
                .trendSide(this.trendSide)
                .bidUserId(this.bidUserId)
                .askUserId(this.askUserId)
                .buyFee(this.buyFee)
                .sellFee(this.sellFee)
                .buyFeeCoin(this.buyFeeCoin)
                .sellFeeCoin(this.sellFeeCoin)
                .ctime(this.ctime)
                .mtime(this.mtime)
                .buyType(this.buyType)
                .sellType(this.sellType)
                .tradeNonce(this.tradeNonce)
                .build();
    }

    @JsonIgnore
    public ExOrderEntity toMMExOrderEntity(Integer mmChainupId, OrderSide orderSide) {
        LocalDateTime eventTime = LocalDateTime.now();
        BigDecimal dealMoney = this.volume.multiply(this.price);
        return ExOrderEntity.builder()
                .userId(mmChainupId)
                .side(orderSide)
                .price(this.price)
                .volume(this.volume)
                .feeDeductType(FeeDeductType.INNER)
                .feeRateMaker(0d)
                .feeRateTaker(0d)
                .fee(BigDecimal.ZERO)
                .feeCoinRate(0d)
                .dealVolume(this.volume)
                .dealMoney(dealMoney)
                .avgPrice(this.price)
                .lockedAmount(OrderSide.BUY == orderSide ? dealMoney : this.volume)
                .status(OrderStatus.FILLED)
                .type(OrderType.LIMIT)
                .source(OrderSourceType.ROBOT)
                .orderType(OrderLeverType.MARKET_MAKING_ORDER)
                .ctime(eventTime).mtime(eventTime)
                .build();
    }
}
