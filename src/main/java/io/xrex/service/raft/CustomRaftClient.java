package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import io.xrex.enums.ReadConsistency;
import io.xrex.dto.AccountIdDto;
import io.xrex.dto.event.TransactionEventDto;
import io.xrex.dto.raft.BatchCommand;
import io.xrex.dto.raft.QueryCommand;
import io.xrex.util.UUIDv7Generator;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A client for interacting with the Raft cluster.
 * This class encapsulates the logic for sending commands (both write and read) to the Raft group.
 * It handles the creation of Raft-specific commands and manages client-side idempotency
 * with a unique client ID and per-request sequence numbers.
 */
public class CustomRaftClient {

    private final RaftClient client;
    // A unique ID for this client instance, used for idempotency in the state machine.
    private final String clientId = UUIDv7Generator.generate();
    // A sequence number generator for requests, also used for idempotency checks.
    private final AtomicLong sequenceIdGenerator = new AtomicLong();


    public CustomRaftClient(RaftGroup raftGroup) {
        final RaftProperties properties = new RaftProperties();
        // Increase the request timeout to 15 seconds to allow for server warm-up.
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(15, TimeUnit.SECONDS));

        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(raftGroup)
                .build();
    }

    /**
     * Closes the Raft client and releases any resources.
     * @throws Exception if an error occurs during closing.
     */
    public void close() throws Exception {
        client.close();
    }

    /**
     * Sends a batch of transfer events to the Raft cluster for processing.
     * This is the entry point for all write operations.
     *
     * @param events A list of transfer events.
     * @return A CompletableFuture that will be completed with the Raft client's reply.
     */
    public CompletableFuture<RaftClientReply> sendBatch(List<TransactionEventDto> events) {
        // 1. Generate a unique sequence number for this batch request.
        long sequenceId = sequenceIdGenerator.getAndIncrement();
        // 2. Encapsulate the client ID, sequence ID, and transfer events into a BatchCommand.
        BatchCommand command = new BatchCommand(clientId, sequenceId, events);
        // 3. Asynchronously send the command to the Raft cluster.
        //    The RaftClient automatically routes the request to the leader node.
        //    The leader then starts the Raft log replication process.
        return client.async().send(Message.valueOf(ByteString.copyFrom(JSON.toJSONBytes(command))));
    }

    /**
     * Queries an account balance with a specified consistency level.
     * This is the entry point for all read operations.
     *
     * @param accountId The ID of the account to query.
     * @param readConsistency The desired read consistency level (STRONG, BOUNDED, EVENTUAL).
     * @return A CompletableFuture that will be completed with the Raft client's reply.
     */
    public CompletableFuture<RaftClientReply> queryBalance(AccountIdDto accountId, ReadConsistency readConsistency) {
        long sequenceId = sequenceIdGenerator.getAndIncrement();
        // 1. Encapsulate the query account, consistency level, etc., into a QueryCommand.
        QueryCommand command = new QueryCommand(clientId, sequenceId, accountId, readConsistency);
        // 2. Send a read-only request to the Raft cluster.
        //    - STRONG: The request is sent to the leader to ensure the latest committed data is read.
        //    - BOUNDED/EVENTUAL: The request may be sent to a follower, tolerating some data staleness
        //      in exchange for lower latency and load.
        return client.async().sendReadOnly(Message.valueOf(ByteString.copyFrom(JSON.toJSONBytes(command))));
    }
}
