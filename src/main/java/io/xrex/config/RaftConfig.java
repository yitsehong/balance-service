package io.xrex.config;

import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import io.xrex.service.kafka.KafkaProducerService;
import io.xrex.service.raft.BalanceStateMachine;
import io.xrex.service.raft.CustomRaftClient;
import io.xrex.service.raft.CustomRaftServer;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

@Configuration
public class RaftConfig {

    @Value("${raft.id}")
    private String raftId;

    @Value("${raft.group.id}")
    private String raftGroupId;

    @Value("${raft.port}")
    private int raftPort;

    @Bean
    public BalanceStateMachine raftStateMachine(KafkaProducerService kafkaProducerService,
                                                ConfigService configService, RocksDBService rocksDBService) {
        // Pass all required dependencies to the state machine.
        return new BalanceStateMachine(kafkaProducerService, configService, rocksDBService);
    }

    @Bean
    public RaftGroup raftGroup() {
        final RaftPeer peer = RaftPeer.newBuilder()
                .setId(raftId).setAddress("localhost:" + raftPort).build();
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(raftGroupId)), Collections.singletonList(peer));
    }

    @Bean(destroyMethod = "stop")
    public CustomRaftServer raftServer(BalanceStateMachine stateMachine, RaftGroup raftGroup) throws IOException {
        return new CustomRaftServer(stateMachine, raftId, raftGroup, raftPort);
    }

    @Bean(destroyMethod = "close")
    public CustomRaftClient raftClient(RaftGroup raftGroup) {
        return new CustomRaftClient(raftGroup);
    }
}
