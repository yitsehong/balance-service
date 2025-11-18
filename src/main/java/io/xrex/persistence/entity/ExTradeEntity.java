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
public class ExTradeEntity {
    private Long id;
    private BigDecimal price;
    private BigDecimal volume;
    private Long bidId;
    private Long askId;
    private String trendSide;
    private Integer bidUserId;
    private Integer askUserId;
    private BigDecimal buyFee;
    private BigDecimal sellFee;
    private String buyFeeCoin;
    private String sellFeeCoin;
    private LocalDateTime ctime;
    private LocalDateTime mtime;
    private Byte buyType;
    private Byte sellType;
}
