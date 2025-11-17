package io.xrex.persistence.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Entity
@Data
@Table(name = "config_coin_symbol")
public class ConfigCoinSymbolEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String coinSymbol;
    private String contractAddress;
    private Integer showPrecision;
    private Byte otcOpen;
    private Byte isFiat;
    private Byte isQuote;
    private Byte isOpen;
    private Byte depositOpen;
    private Byte withdrawOpen;
    private Byte useRate;
    private String tokenBase;
    private String chainAddress;
    private String chainTx;
    private Integer depositConfirm;
    private Integer miningDepositConfirm;
    private BigDecimal withdrawMax;
    private BigDecimal withdrawMin;
    private BigDecimal withdrawMaxDay;
    private BigDecimal withdrawMaxDayNoAuth;
    private String btcRateMath;
    private Integer addressLen;
    private String name;
    private String icon;
    private String link;
    private Integer sort;
    private Byte releaseStatus;
    private Date ctime;
    private Date mtime;
    private Date releaseDate;
    private Byte isRelease;
    private Byte securityStatus;
    private BigDecimal depositMin;
    private Byte tagType;
    private Integer tagFormatType;
    private Byte supportToken;
    private String regular;
    private Byte depositLockOpen;
    private Byte onlyHoldShow;
    private Byte isShow;
    private Byte isBitcheckOpen;
    private Integer coinType;
    private Byte isLoanable;
    private Byte isCollateralable;
    private Byte isOtcConvertOpen;
    private BigDecimal otcConvertMinUsd;
    private BigDecimal otcConvertMaxUsd;
    private Byte isPreferredCurrency;
    private Integer preferredCurrencySort;
    private String hookedCoin;
}
