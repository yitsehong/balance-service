package io.xrex.service.raft;

import io.xrex.dto.event.TransactionEventDto;
import lombok.Getter;
import org.apache.ratis.protocol.RaftClientReply;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a transfer request to be sent to the Raft cluster.
 * This class encapsulates the transaction event data and a {@link CompletableFuture}
 * that will be completed with the reply from the Raft cluster. This allows the
 * calling thread to asynchronously wait for the result of the Raft operation.
 */
@Getter
public class TransferRaftRequest {
    /** The transaction event data. */
    private final TransactionEventDto event;
    /** The future that will be completed with the Raft client reply. */
    private final CompletableFuture<RaftClientReply> future;

    /**
     * Constructs a new TransferRaftRequest.
     *
     * @param event The transaction event data.
     */
    public TransferRaftRequest(TransactionEventDto event) {
        this.event = event;
        this.future = new CompletableFuture<>();
    }
}
