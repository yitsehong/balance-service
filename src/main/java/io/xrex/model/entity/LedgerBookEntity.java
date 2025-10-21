package io.xrex.model.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.xrex.util.BigDecimalToStringSerializer;
import io.xrex.util.XrexConstant;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "ledger_book")
@IdClass(LedgerBookId.class)
public class LedgerBookEntity {
    @Id
    private String idempotencyKey;
    @Id
    private Integer chainupId;
    @Id
    private Integer assetType;

    private String coinSymbol;
    private String accountTag;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal amount;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal beforeBalance;
    @JsonSerialize(using = BigDecimalToStringSerializer.class)
    private BigDecimal afterBalance;
    private String scene;
    private String refType;
    private Long refId;
    @Id
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime createdTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private LocalDateTime updatedTime;
}
