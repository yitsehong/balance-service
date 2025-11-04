package io.xrex.service.raft.command;

import lombok.Data;

import java.io.Serializable;

@Data
public class RaftCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String clientId;
    private final long sequenceId;

    public RaftCommand(String clientId, long sequenceId) {
        this.clientId = clientId;
        this.sequenceId = sequenceId;
    }

}