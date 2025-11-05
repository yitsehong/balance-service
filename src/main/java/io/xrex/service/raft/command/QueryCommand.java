package io.xrex.service.raft.command;

import io.xrex.enums.ReadConsistency;
import io.xrex.model.dto.AccountIdDto;
import lombok.Getter;

@Getter
public class QueryCommand extends RaftCommand {
    private static final long serialVersionUID = 1L;

    private final AccountIdDto accountId;
    private final ReadConsistency readConsistency;

    public QueryCommand(String clientId, long sequenceId, AccountIdDto accountId, ReadConsistency readConsistency) {
        super(clientId, sequenceId);
        this.accountId = accountId;
        this.readConsistency = readConsistency;
    }

}