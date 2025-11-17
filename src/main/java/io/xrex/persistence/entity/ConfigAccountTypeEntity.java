package io.xrex.persistence.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonFormat;
import io.xrex.util.XrexConstant;
import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "config_account_type")
public class ConfigAccountTypeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Integer assetType;

    @Column(name = "asset_a")
    private String assetA;

    private String assetBc;

    private Integer assetCoinNumber;

    private String coinSymbol;

    private String symbol;

    private String tag;

    private String description;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    @JSONField(deserialize = false)
    private LocalDateTime ctime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    @JSONField(deserialize = false)
    private LocalDateTime mtime;
}
