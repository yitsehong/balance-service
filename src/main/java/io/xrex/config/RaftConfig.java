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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    @Value("${raft.group.peers}")
    private String raftGroupPeers;

    @Bean
    public BalanceStateMachine raftStateMachine() {
        return new BalanceStateMachine(kafkaTemplate, configService, rocksDBService);
    }

    @Bean
    public RaftGroup raftGroup() {
        String[] peers = raftGroupPeers.split(",");
        List<RaftPeer> raftPeers = new ArrayList<>();
        for (String peer : peers) {
            String[] parts = peer.split("=");
            String peerId = parts[0];
            String peerAddress = parts[1];
            raftPeers.add(RaftPeer.newBuilder().setId(peerId).setAddress(peerAddress).build());
        }
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(raftGroupId)), raftPeers);
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
