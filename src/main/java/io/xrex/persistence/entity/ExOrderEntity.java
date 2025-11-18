package io.xrex.persistence.entity;


import com.fasterxml.jackson.annotation.JsonIgnore;
import io.xrex.dto.PairConfigDto;
import io.xrex.enums.*;
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
public class ExOrderEntity {
    private Long id;
    private Integer userId;
    private OrderSide side;
    private BigDecimal price;
    private BigDecimal volume;
    private Integer feeAccountType;
    private FeeDeductType feeDeductType;
    private Double feeRateMaker;
    private Double feeRateTaker;
    private BigDecimal fee;
    private Double feeCoinRate;
    private BigDecimal dealVolume;
    private BigDecimal dealMoney;
    private BigDecimal avgPrice;
    private BigDecimal lockedAmount;
    private OrderStatus status;
    private OrderType type;
    private LocalDateTime ctime;
    private LocalDateTime mtime;
    private OrderSourceType source;
    private OrderLeverType orderType;
    private BigDecimal stopPrice;
    private StopPriceDirection stopPriceDirection;
    private Integer quoteAccountType;
    private String quoteSubaccountType;
    private Integer baseAccountType;
    private String baseSubaccountType;
    private Long marginTradeId;
    private String marginDirection;
    private Long botId;

    @JsonIgnore
    public boolean isFilled(PairConfigDto pairConfig) {
        // Based on pair config base & quote precision to determine min amount
        BigDecimal minBaseAmount = pairConfig.getMinBaseAmount();
        BigDecimal minQuoteAmount = pairConfig.getMinQuoteAmount();

        if (this.isLimitOrder()) {
            return getUnfilledQuantity().compareTo(minBaseAmount) < 0;
        } else if (this.isMarketOrder()) {
            if (OrderSide.BUY == this.getSide()) {
                BigDecimal unfilledAmount;
                if (FeeDeductType.INNER == this.feeDeductType) {
                    unfilledAmount = this.volume.subtract(this.dealMoney);
                    return unfilledAmount.compareTo(minQuoteAmount) < 0;
                } else {
                    BigDecimal takerFeeRate = new BigDecimal(this.feeRateTaker.toString());
                    BigDecimal unfilledLockedAmount = this.lockedAmount.subtract(this.dealMoney.multiply(BigDecimal.ONE.add(takerFeeRate)));
                    return getUnfilledQuantity().compareTo(minBaseAmount) < 0 || unfilledLockedAmount.compareTo(minQuoteAmount) < 0;
                }
            } else {
                return getUnfilledQuantity().compareTo(minBaseAmount) < 0;
            }
        }
        return false;
    }

    @JsonIgnore
    public boolean isLimitOrder() {
        return this.type == OrderType.LIMIT || this.type == OrderType.STOP_LIMIT;
    }

    @JsonIgnore
    public boolean isMarketOrder() {
        return this.type == OrderType.MARKET;
    }

    @JsonIgnore
    public boolean isRelatedSubAccount() {
        return this.orderType == OrderLeverType.GRID_ORDER
                || this.orderType == OrderLeverType.MARGIN_ORDER
                || this.orderType == OrderLeverType.GRID_MARGIN_ORDER;
    }

    @JsonIgnore
    public boolean isInnerFeeDeduct() {
        return FeeDeductType.INNER == this.feeDeductType;
    }

    @JsonIgnore
    public boolean isOuterFeeDeduct() {
        return FeeDeductType.OUTER == this.feeDeductType;
    }

    @JsonIgnore
    public BigDecimal getUnfilledQuantity() {
        return this.volume.subtract(this.dealVolume);
    }

    @JsonIgnore
    public boolean isMarginOrder() {
        return this.orderType == OrderLeverType.MARGIN_ORDER;
    }

    @JsonIgnore
    public boolean isGridOrder() {
        return this.orderType == OrderLeverType.GRID_ORDER;
    }

    @JsonIgnore
    public boolean isGridMarginOrder() {
        return this.orderType == OrderLeverType.GRID_MARGIN_ORDER;
    }

    @JsonIgnore
    public boolean isConvertOrder() {
        return this.orderType == OrderLeverType.CONVERT_ORDER;
    }

    @JsonIgnore
    public BigDecimal getRemainAmount() {
        BigDecimal remainAmount;
        if (this.isMarketOrder() && this.isInnerFeeDeduct()) {
            if (this.side == OrderSide.BUY) {
                remainAmount = this.volume.subtract(this.dealMoney);
            } else {
                remainAmount = this.volume.subtract(this.dealVolume);
            }
        } else if (this.isMarketOrder() && this.isOuterFeeDeduct()) {
            if (this.side == OrderSide.BUY) {
                remainAmount = this.lockedAmount.subtract(this.dealMoney).subtract(this.fee);
            } else {
                remainAmount = this.getUnfilledQuantity();
            }
        } else {
            if (this.side == OrderSide.BUY) {
                if (this.isInnerFeeDeduct()) {
                    remainAmount = this.price.multiply(this.volume).subtract(this.dealMoney);
                } else {
                    remainAmount = this.lockedAmount.subtract(this.dealMoney).subtract(this.fee);
                }
            } else {
                remainAmount = this.getUnfilledQuantity();
            }
        }
        return remainAmount;
    }
}