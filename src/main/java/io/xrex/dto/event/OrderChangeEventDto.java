package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.util.XrexConstant;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrderChangeEventDto {

    private String pair;
    private Long orderId;
    private Integer userId;
    private String side;
    private Byte orderType;
    private BigDecimal dealVolume;
    private BigDecimal dealMoney;
    private BigDecimal dealFee;
    private BigDecimal avgPrice;
    private Boolean isFilled;
    private Boolean isCancelled;

    private Integer feeAccountType;
    private Integer feeDeductType;
    private Integer quoteAccountType;
    private String quoteSubaccountType;
    private Integer baseAccountType;
    private String baseSubaccountType;
    private BigDecimal feeRateMaker;
    private BigDecimal feeRateTaker;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime eventTime;

    private List<ExTradeEntity> trades;
}
