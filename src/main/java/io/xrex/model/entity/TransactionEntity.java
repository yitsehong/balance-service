package io.xrex.model.entity;

import io.xrex.util.MD5Util;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "transaction")
public class TransactionEntity {
    @Id
    private Long id;
    private Integer fromUid;
    private Integer fromType;
    private BigDecimal fromBalance;
    private Integer toUid;
    private Integer toType;
    private BigDecimal toBalance;
    private BigDecimal amount;
    private String meta;
    private String scene;
    private String refType;
    private Long refId;
    private Integer opUid;
    private String opIp;
    private LocalDateTime ctime;
    private LocalDateTime mtime;
    private String fingerprint;

    /**
     * 使用默认key计算指纹信息
     *
     * @author mfXing
     */
    public void fingerprint() {
        fingerprint(StringUtils.EMPTY);
    }

    private void fingerprint(String key) {
        this.setFingerprint(calcFingerprint(key));
    }

    /**
     * 防篡改指纹
     *
     * @param key
     * @author mfXing
     */
    private String calcFingerprint(String key) {
        String[] elements = {fromUid.toString(), fromType.toString(),
                fromBalance == null ? BigDecimal.ZERO.stripTrailingZeros().toPlainString() : fromBalance.stripTrailingZeros().toPlainString(),
                toUid.toString(), toType.toString(),
                toBalance == null ? BigDecimal.ZERO.stripTrailingZeros().toPlainString() : toBalance.stripTrailingZeros().toPlainString(),
                amount == null ? BigDecimal.ZERO.stripTrailingZeros().toPlainString() : amount.stripTrailingZeros().toPlainString(), key};
        String info = String.join("_", elements);
        return MD5Util.getMD5(info);
    }

}
