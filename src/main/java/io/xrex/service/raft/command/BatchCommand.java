package io.xrex.service.raft.command;

import io.xrex.model.dto.event.TransactionEventDto;
import lombok.Getter;

import java.util.List;

@Getter
public class BatchCommand extends RaftCommand {
    private static final long serialVersionUID = 1L;

    private final List<TransactionEventDto> events;

    public BatchCommand(String clientId, long sequenceId, List<TransactionEventDto> events) {
        super(clientId, sequenceId);
        this.events = events;
    }

}