package io.xrex.model.dto;

import java.io.Serializable;

public record AccountIdDto(Integer chainupId, Integer assetType) implements Serializable, Comparable<AccountIdDto> {
    @Override
    public int compareTo(AccountIdDto other) {
        int uidCompare = this.chainupId.compareTo(other.chainupId);
        if (uidCompare != 0) {
            return uidCompare;
        }
        return Integer.compare(this.assetType, other.assetType);
    }
}
