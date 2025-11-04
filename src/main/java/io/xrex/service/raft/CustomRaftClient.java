package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import io.xrex.enums.ReadConsistency;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.raft.command.BatchCommand;
import io.xrex.service.raft.command.QueryCommand;
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
 * Raft 客戶端，作為應用層與 Raft 叢集的溝通橋樑。
 * 負責將業務請求 (如轉帳、查詢) 封裝成 Raft 指令並發送出去。
 */
public class CustomRaftClient {

    private final RaftClient client;
    // 為每個客戶端實例產生唯一的 ID，用於在狀態機中實現冪等性。
    private final String clientId = UUIDv7Generator.generate();
    // 為每個請求產生唯一的序列號，同樣用於冪等性檢查。
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

    public void close() throws Exception {
        client.close();
    }

    /**
     * 發送一批轉帳交易到 Raft 叢集進行處理。
     * 這是寫入操作的入口點。
     *
     * @param events 轉帳事件列表
     * @return 一個 CompletableFuture，其中包含 Raft 的回覆
     */
    public CompletableFuture<RaftClientReply> sendBatch(List<TransactionEventDto> events) {
        // 1. 產生此次批次請求的唯一序列號
        long sequenceId = sequenceIdGenerator.getAndIncrement();
        // 2. 將 clientId, sequenceId 和轉帳事件列表封裝成一個 BatchCommand
        BatchCommand command = new BatchCommand(clientId, sequenceId, events);
        // 3. 異步發送指令到 Raft 叢集。
        //    RaftClient 會自動將請求路由到 Leader 節點。
        //    Leader 收到後會開始 Raft 的日誌複製流程。
        return client.async().send(Message.valueOf(ByteString.copyFrom(JSON.toJSONBytes(command))));
    }

    /**
     * 根據指定的一致性級別查詢帳戶餘額。
     * 這是讀取操作的入口點。
     *
     * @param accountId       要查詢的帳戶 ID
     * @param readConsistency 讀取一致性級別 (STRONG, BOUNDED, EVENTUAL)
     * @return 一個 CompletableFuture，其中包含 Raft 的回覆
     */
    public CompletableFuture<RaftClientReply> queryBalance(AccountIdDto accountId, ReadConsistency readConsistency) {
        long sequenceId = sequenceIdGenerator.getAndIncrement();
        // 1. 將查詢帳戶、一致性級別等資訊封裝成 QueryCommand
        QueryCommand command = new QueryCommand(clientId, sequenceId, accountId, readConsistency);
        // 2. 發送一個唯讀請求到 Raft 叢集。
        //    - STRONG: 請求會發給 Leader，確保讀到最新的已提交數據。
        //    - BOUNDED/EVENTUAL: 請求可能會發給 Follower，容忍一定程度的數據延遲以換取更低的延遲和負載。
        return client.async().sendReadOnly(Message.valueOf(ByteString.copyFrom(JSON.toJSONBytes(command))));
    }
}
