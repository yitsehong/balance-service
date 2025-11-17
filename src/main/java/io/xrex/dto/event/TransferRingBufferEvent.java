package io.xrex.dto.event;

import lombok.Data;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

@Data
public class TransferRingBufferEvent {

    private String transactionId;
    private Integer fromChainupId;
    private Integer fromAssetType;
    private Integer toChainupId;
    private Integer toAssetType;
    private BigDecimal amount;
    private String scene;
    private String meta;
    private String refType;
    private Long refId;
    private Integer opUid;
    private String opIp;

    // 用於異步轉同步，通知調用方處理結果
    private CompletableFuture<String> future;

    public void clear() {
        transactionId = null;
        fromChainupId = null;
        fromAssetType = null;
        toChainupId = null;
        toAssetType = null;
        amount = null;
        scene = null;
        meta = null;
        refType = null;
        refId = null;
        opUid = null;
        opIp = null;
        future = null;
    }
}
