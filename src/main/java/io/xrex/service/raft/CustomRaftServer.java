package io.xrex.service.raft;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

@Slf4j
public class CustomRaftServer {

    private final RaftServer server;

    public CustomRaftServer(BalanceStateMachine stateMachine, String raftId, RaftGroup raftGroup, int port) throws IOException {
        // Create a new RaftProperties object
        final RaftProperties properties = new RaftProperties();

        // Set the storage directory
        final File storageDir = new File("/tmp/" + raftId);

        // DEV-ONLY: Clean up storage directory on startup to prevent format errors
        // TODO OPTIMIZE
        if (storageDir.exists()) {
            log.warn("DEVELOPMENT MODE: Deleting existing Raft storage directory: {}", storageDir);
            try {
                Files.walk(storageDir.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                log.error("Failed to delete Raft storage directory.", e);
                throw e;
            }
        }

        properties.set("raft.server.storage.dir", storageDir.getAbsolutePath());

        // Set the gRPC port
        GrpcConfigKeys.Server.setPort(properties, port);

        // Create the RaftPeer object
        final RaftPeer peer = RaftPeer.newBuilder()
                .setId(raftId)
                .setAddress(NetUtils.createSocketAddr("localhost", port))
                .build();

        // Create the RaftServer object
        this.server = RaftServer.newBuilder()
                .setServerId(peer.getId())
                .setGroup(raftGroup)
                .setProperties(properties)
                .setStateMachine(stateMachine)
                .build();
    }

    @PostConstruct
    public void start() throws IOException {
        server.start();
    }

    public void stop() throws IOException {
        server.close();
    }
}
