package io.xrex.dto.raft;

import lombok.Data;

import java.io.Serializable;

/**
 * The base class for all commands sent to the Raft cluster.
 * It contains common fields used for idempotency, such as the client ID and a sequence number.
 */
@Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class RaftCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    /** A unique identifier for the client submitting the command. */
    private String clientId;
    /** A sequence number for the command from a specific client, used for idempotency. */
    private long sequenceId;


}