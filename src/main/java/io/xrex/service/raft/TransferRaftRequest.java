package io.xrex.service.raft;

import io.xrex.model.dto.event.TransactionEventDto;
import lombok.Getter;
import org.apache.ratis.protocol.RaftClientReply;

import java.util.concurrent.CompletableFuture;

@Getter
public class TransferRaftRequest {
    private final TransactionEventDto event;
    private final CompletableFuture<RaftClientReply> future;

    public TransferRaftRequest(TransactionEventDto event) {
        this.event = event;
        this.future = new CompletableFuture<>();
    }
}
