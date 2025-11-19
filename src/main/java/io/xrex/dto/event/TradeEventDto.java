package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.enums.*;
import io.xrex.persistence.entity.ExOrderEntity;
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
public class TradeEventDto {

    private String pair;
    private Long orderId;
    private Integer chainupId;
    private OrderSide orderSide;
    private ExTradeDto trade;
    @JsonIgnore
    private List<ExTradeDto> trades;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime eventTime;
}
