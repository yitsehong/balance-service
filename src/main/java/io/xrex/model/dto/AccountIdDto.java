package io.xrex.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountIdDto implements Serializable, Comparable<AccountIdDto> {

    private Integer chainupId;
    private Integer assetType;
    private String coinSymbol;
    private String accountTag;

    @Override
    public int compareTo(AccountIdDto other) {
        int uidCompare = this.chainupId.compareTo(other.chainupId);
        if (uidCompare != 0) {
            return uidCompare;
        }
        return Integer.compare(this.assetType, other.assetType);
    }

    @Override
    public String toString() {
        return "AccountIdDto{" +
                "chainupId=" + chainupId +
                ", assetType=" + assetType +
                '}';
    }
}
