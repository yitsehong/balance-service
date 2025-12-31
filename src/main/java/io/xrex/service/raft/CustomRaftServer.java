package io.xrex.service.raft;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * A wrapper class for the Apache Ratis RaftServer.
 * This class simplifies the configuration and lifecycle management of a Raft server instance.
 * It sets up the necessary properties, storage directories, and network configurations
 * for a peer in the Raft group.
 */
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
        // Force NIO to avoid Epoll EventLoopGroup creation failures
        // Setting properties directly as specific setter methods might vary by Ratis version
        properties.set("raft.grpc.server.use.epoll", "false");
        properties.set("raft.grpc.client.use.epoll", "false");

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

    /**
     * Starts the Raft server.
     * @throws IOException if an I/O error occurs during startup.
     */
    public void start() throws IOException {
        server.start();
    }

    /**
     * Stops the Raft server and releases its resources.
     * @throws IOException if an I/O error occurs during shutdown.
     */
    public void stop() throws IOException {
        server.close();
    }
}
