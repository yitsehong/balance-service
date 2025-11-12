package io.xrex.model.dto.raft;

import lombok.Data;

import java.io.Serializable;

/**
 * The base class for all commands sent to the Raft cluster.
 * It contains common fields used for idempotency, such as the client ID and a sequence number.
 */
@Data
public class RaftCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    /** A unique identifier for the client submitting the command. */
    private final String clientId;
    /** A sequence number for the command from a specific client, used for idempotency. */
    private final long sequenceId;

    /**
     * Constructs a new RaftCommand.
     *
     * @param clientId The ID of the client.
     * @param sequenceId The sequence number of the command.
     */
    public RaftCommand(String clientId, long sequenceId) {
        this.clientId = clientId;
        this.sequenceId = sequenceId;
    }

}