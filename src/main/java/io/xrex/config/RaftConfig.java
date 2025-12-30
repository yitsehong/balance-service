package io.xrex.config;

import io.xrex.dto.event.TransactionEventDto;
import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import io.xrex.service.raft.BalanceStateMachine;
import io.xrex.service.raft.CustomRaftClient;
import io.xrex.service.raft.CustomRaftServer;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
public class RaftConfig {

    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;

    @Value("${raft.id}")
    private String raftId;

    @Value("${raft.group.id}")
    private String raftGroupId;

    @Value("${raft.port}")
    private int raftPort;

    @Bean
    public BalanceStateMachine raftStateMachine() {
        return new BalanceStateMachine(kafkaTemplate, configService, rocksDBService);
    }

    @Bean
    public RaftGroup raftGroup() {
        final RaftPeer peer = RaftPeer.newBuilder()
                .setId(raftId).setAddress("http://127.0.0.1:" + raftPort).build();
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(raftGroupId)), Collections.singletonList(peer));
    }

    @Bean(destroyMethod = "stop")
    public CustomRaftServer raftServer(BalanceStateMachine stateMachine, RaftGroup raftGroup) throws IOException {
        CustomRaftServer server = new CustomRaftServer(stateMachine, raftId, raftGroup, raftPort);
        server.start();
        return server;
    }

    @Bean(destroyMethod = "close")
    @org.springframework.context.annotation.DependsOn("raftServer")
    public CustomRaftClient raftClient(RaftGroup raftGroup) {
        return new CustomRaftClient(raftGroup);
    }
}
