package company.vk.edu.distrib.compute.khetagab;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KhetagReplicatedService implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(KhetagReplicatedService.class);
    private static final Path STORAGE_BASE = Path.of("khetag-ab-storage");

    private final int servicePort;
    private final int replicaCount;
    private final HttpServer server;
    private final List<ReplicaStore> replicas;
    private final List<AtomicBoolean> replicaEnabled;
    private final ExecutorService replicaOpExecutor;

    public KhetagReplicatedService(int port) throws IOException {
        this.servicePort = port;
        this.replicaCount = KhetagReplicaConfig.replicaCount();
        long replicaOpTimeoutMs = KhetagReplicaConfig.replicaOpTimeoutMs();
        Files.createDirectories(STORAGE_BASE);
        List<ReplicaStore> replicaStores = new ArrayList<>(replicaCount);
        List<AtomicBoolean> enabledFlags = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            Path root = Files.createTempDirectory(STORAGE_BASE, "replica-" + i + "-");
            replicaStores.add(new VersionedFileReplicaStore(root));
            enabledFlags.add(new AtomicBoolean(true));
        }
        this.replicas = List.copyOf(replicaStores);
        this.replicaEnabled = List.copyOf(enabledFlags);
        this.replicaOpExecutor = Executors.newFixedThreadPool(Math.max(4, replicaCount));
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new KhetagStatusHandler());
        server.createContext(
                "/v0/entity",
                new ReplicatedEntityHandler(
                        replicas, replicaEnabled, replicaCount, replicaOpExecutor, replicaOpTimeoutMs));
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return replicaCount;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaEnabled.get(nodeId).set(false);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaEnabled.get(nodeId).set(true);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        replicaOpExecutor.shutdown();
        try {
            if (!replicaOpExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicaOpExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            replicaOpExecutor.shutdownNow();
        }
        for (ReplicaStore store : replicas) {
            if (store instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    log.warn("replica store close failed", e);
                }
            }
        }
    }
}
