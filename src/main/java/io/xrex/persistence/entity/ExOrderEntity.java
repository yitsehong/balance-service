package io.xrex.persistence.entity;


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
    private String side;
    private BigDecimal price;
    private BigDecimal volume;
    private Integer feeAccountType;
    private Integer feeDeductType;
    private Double feeRateMaker;
    private Double feeRateTaker;
    private BigDecimal fee;
    private Double feeCoinRate;
    private BigDecimal dealVolume;
    private BigDecimal dealMoney;
    private BigDecimal avgPrice;
    private BigDecimal lockedAmount;
    private Byte status;
    private Byte type;
    private LocalDateTime ctime;
    private LocalDateTime mtime;
    private Byte source;
    /**
     * 订单类型1:常规订单，2 杠杆订单
     */
    private Byte orderType;

    private BigDecimal stopPrice;
    private Byte stopPriceDirection;
    private Integer quoteAccountType;
    private String quoteSubaccountType;
    private Integer baseAccountType;
    private String baseSubaccountType;
    private Long marginTradeId;
    private String marginDirection;
    private Long botId;

}