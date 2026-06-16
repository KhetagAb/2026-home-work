package company.vk.edu.distrib.compute.khetagab.cluster;

import com.sun.net.httpserver.HttpServer;
import com.google.common.hash.Hashing;
import company.vk.edu.distrib.compute.khetagab.handler.KhetagStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class KhetagClusterNode {

    private final int httpPort;
    private final int grpcPort;
    private final List<NodeInfo> allNodes;
    private final Map<String, byte[]> storage;
    private final Map<String, Boolean> tombstones;
    private final Map<Integer, ManagedChannel> peerChannels;
    private HttpServer httpServer;
    private Server grpcServer;

    KhetagClusterNode(int httpPort, int grpcPort, List<NodeInfo> allNodes) {
        this.httpPort = httpPort;
        this.grpcPort = grpcPort;
        this.allNodes = List.copyOf(allNodes);
        this.storage = new ConcurrentHashMap<>();
        this.tombstones = new ConcurrentHashMap<>();
        this.peerChannels = new HashMap<>();
    }

    synchronized void start() throws IOException {
        grpcServer = ServerBuilder.forPort(grpcPort)
            .addService(new KhetagKvGrpcService(storage, tombstones))
            .build()
            .start();
        for (NodeInfo peer : allNodes) {
            if (peer.httpPort() != httpPort) {
                ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", peer.grpcPort())
                    .usePlaintext()
                    .build();
                peerChannels.put(peer.grpcPort(), channel);
            }
        }
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/v0/status", new KhetagStatus());
        httpServer.createContext("/v0/entity", new KhetagClusterEntity(this));
        httpServer.start();
    }

    synchronized void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                grpcServer.shutdownNow();
            }
            grpcServer = null;
        }
        for (ManagedChannel ch : peerChannels.values()) {
            ch.shutdown();
        }
        peerChannels.clear();
    }

    boolean isLocalOwner(String key) {
        return findOwnerHttpPort(key) == httpPort;
    }

    ManagedChannel channelToOwner(String key) {
        int ownerHttpPort = findOwnerHttpPort(key);
        for (NodeInfo node : allNodes) {
            if (node.httpPort() == ownerHttpPort) {
                return peerChannels.get(node.grpcPort());
            }
        }
        return null;
    }

    byte[] localGet(String key) {
        if (tombstones.containsKey(key)) {
            throw new NoSuchElementException(key);
        }
        byte[] val = storage.get(key);
        if (val == null) {
            throw new NoSuchElementException(key);
        }
        return val;
    }

    void localPut(String key, byte[] value) {
        tombstones.remove(key);
        storage.put(key, value);
    }

    void localDelete(String key) {
        storage.remove(key);
        tombstones.put(key, Boolean.TRUE);
    }

    private int findOwnerHttpPort(String key) {
        int bestPort = -1;
        long bestHash = Long.MIN_VALUE;
        for (NodeInfo node : allNodes) {
            long hash = hrwHash(node.httpPort(), key);
            if (hash > bestHash) {
                bestHash = hash;
                bestPort = node.httpPort();
            }
        }
        return bestPort;
    }

    private static long hrwHash(int nodePort, String key) {
        String combined = nodePort + "|" + key;
        return Hashing.murmur3_128().hashString(combined, StandardCharsets.UTF_8).asLong();
    }
}
