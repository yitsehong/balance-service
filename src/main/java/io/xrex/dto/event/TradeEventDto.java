package io.xrex.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.util.XrexConstant;
import lombok.*;

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
    private ExTradeDto trade;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime eventTime;
}
