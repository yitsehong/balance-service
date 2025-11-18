package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.xrex.persistence.entity.ExTradeEntity;
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
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime ctime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime mtime;
    private Byte buyType;
    private Byte sellType;

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
                .build();
    }
}
