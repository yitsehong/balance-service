package io.xrex.persistence.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.xrex.util.BigDecimalToStringSerializer;
import io.xrex.util.DoubleToStringSerializer;
import io.xrex.util.XrexConstant;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
@Entity
@Table(name = "config_symbol")
public class ConfigSymbolEntity {
    @Id
    private Integer id;
    private String symbol;
    private String base;
    private String quote;
    private Byte isOpen;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal openPrice;
    private Byte isFiat;
    @Column(name = "depth0_pre")
    private Integer depth0Pre;
    @Column(name = "depth1_pre")
    private Integer depth1Pre;
    @Column(name = "depth2_pre")
    private Integer depth2Pre;
    private Integer pricePre;
    private Integer volumePre;
    private Integer depthFullVolume;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal limitPriceMin;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal limitVolumeMin;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal marketBuyMin;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal marketSellMin;
    private Integer sort;
    private String description;
    private Byte releaseStatus;
    private Byte isRelease;
    private Byte isOpenLever;
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = XrexConstant.ISO_DATE_FORMAT
    )
    private Date ctime;
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = XrexConstant.ISO_DATE_FORMAT
    )
    private Date mtime;
    private Byte newcoinFlag;
    private Byte isShow;
    private Byte isIndexShow;
    private Byte isConvertOnly;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal marginRatioMarginCall;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal marginRatioLiquidation;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal marginRatioWarning;
    private Boolean enableMarginTrade;
    private Boolean enableGridMargin;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double marginTradeBaseMarginCostRate;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double marginTradeQuoteMarginCostRate;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double marginTradeLoanDifferentCoinRatio;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double marginMaxLeverage;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double gridMarginMaxLeverage;
    private Boolean enableGridTrade;
    @JsonSerialize(
            using = DoubleToStringSerializer.class
    )
    private Double spotMarketOrderOuterFeeDeductBuffer;
    private Integer symbolType;
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd"
    )
    private Date releaseDate;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal priceDeviationRatioBuyHigh;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal priceDeviationRatioBuyLow;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal priceDeviationRatioSellHigh;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal priceDeviationRatioSellLow;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal reasonablePriceVolatilityRatioMultiplier;
    @JsonSerialize(
            using = BigDecimalToStringSerializer.class
    )
    private BigDecimal priceDeviationMinimumThreshold;
}
