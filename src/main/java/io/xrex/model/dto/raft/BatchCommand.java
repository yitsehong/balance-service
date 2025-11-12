package io.xrex.model.dto.raft;

import io.xrex.model.dto.event.TransactionEventDto;
import lombok.Getter;

import java.util.List;

/**
 * A Raft command that encapsulates a batch of transaction events.
 * This command is used for write operations, allowing multiple transfers to be
 * processed together as a single entry in the Raft log.
 */
@Getter
public class BatchCommand extends RaftCommand {
    private static final long serialVersionUID = 1L;

    /** The list of transaction events in this batch. */
    private final List<TransactionEventDto> events;

    /**
     * Constructs a new BatchCommand.
     *
     * @param clientId The ID of the client submitting the command.
     * @param sequenceId The sequence number for this command, used for idempotency.
     * @param events The list of transaction events.
     */
    public BatchCommand(String clientId, long sequenceId, List<TransactionEventDto> events) {
        super(clientId, sequenceId);
        this.events = events;
    }

}