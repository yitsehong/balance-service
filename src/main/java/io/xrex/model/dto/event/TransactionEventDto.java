package io.xrex.model.dto.event;

import io.xrex.model.entity.LedgerBookEntity;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TransactionEventDto {

    private String eventKey;
    private String meta;
    private Integer opUid;
    private String opIp;
    private LedgerBookEntity from;
    private LedgerBookEntity to;

}
