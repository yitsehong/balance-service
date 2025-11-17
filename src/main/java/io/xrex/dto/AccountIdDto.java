package io.xrex.dto;

import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountIdDto implements Serializable, Comparable<AccountIdDto> {

    private Integer chainupId;
    private Integer assetType;
    private String coinSymbol;
    private String accountTag;

    public AccountIdDto(Integer chainupId, ConfigAccountTypeEntity configAccountType) {
        this.chainupId = chainupId;
        this.assetType = configAccountType.getAssetType();
        this.coinSymbol = configAccountType.getCoinSymbol();
        this.accountTag = configAccountType.getTag();
    }

    @Override
    public int compareTo(AccountIdDto other) {
        int uidCompare = this.chainupId.compareTo(other.chainupId);
        if (uidCompare != 0) {
            return uidCompare;
        }
        return Integer.compare(this.assetType, other.assetType);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AccountIdDto that = (AccountIdDto) o;
        return Objects.equals(chainupId, that.chainupId) && Objects.equals(assetType, that.assetType) && Objects.equals(coinSymbol, that.coinSymbol) && Objects.equals(accountTag, that.accountTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chainupId, assetType, coinSymbol, accountTag);
    }

    @Override
    public String toString() {
        return "AccountIdDto{" +
                "chainupId=" + chainupId +
                ", assetType=" + assetType +
                ", coinSymbol='" + coinSymbol + '\'' +
                ", accountTag='" + accountTag + '\'' +
                '}';
    }
}
