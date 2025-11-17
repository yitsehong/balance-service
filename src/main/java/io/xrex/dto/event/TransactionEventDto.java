package io.xrex.dto.event;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.persistence.entity.LedgerBookEntity;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TransactionEventDto {

    private String transactionId;
    private String meta;
    private Integer opUid;
    private String opIp;
    private LedgerBookEntity from;
    private LedgerBookEntity to;

}
