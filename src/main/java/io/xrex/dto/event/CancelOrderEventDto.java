package io.xrex.dto.event;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.enums.OrderLeverType;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CancelOrderEventDto {
    private String pair;
    private Long orderId;
    private Integer chainupId;
    private OrderLeverType orderType;
}
