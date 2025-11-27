package io.xrex.dto.raft;

import io.xrex.enums.ReadConsistency;
import io.xrex.dto.AccountIdDto;
import lombok.Getter;

/**
 * A Raft command for querying account balances.
 * This command is used for read operations and includes the account to be queried
 * and the desired level of read consistency.
 */
@Getter
@lombok.Setter
@lombok.NoArgsConstructor
public class QueryCommand extends RaftCommand {
    private static final long serialVersionUID = 1L;

    /** The ID of the account to be queried. */
    private AccountIdDto accountId;
    /** The desired level of read consistency for the query. */
    private ReadConsistency readConsistency;

    /**
     * Constructs a new QueryCommand.
     *
     * @param clientId The ID of the client submitting the command.
     * @param sequenceId The sequence number for this command.
     * @param accountId The ID of the account to query.
     * @param readConsistency The desired read consistency.
     */
    public QueryCommand(String clientId, long sequenceId, AccountIdDto accountId, ReadConsistency readConsistency) {
        super(clientId, sequenceId);
        this.accountId = accountId;
        this.readConsistency = readConsistency;
    }

}