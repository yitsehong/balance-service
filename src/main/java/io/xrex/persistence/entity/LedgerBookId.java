package io.xrex.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LedgerBookId {

    private String idempotencyKey;
    private Integer chainupId;
    private Integer assetType;
    private LocalDateTime createdTime;
}
