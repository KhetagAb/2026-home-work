package company.vk.edu.distrib.compute.khetagab.replica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.khetagab.EntityQueryUtils;
import company.vk.edu.distrib.compute.khetagab.HttpCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReplicatedEntityHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedEntityHandler.class);
    private static final int NO_BODY = -1;

    private final List<AtomicBoolean> replicaEnabled;
    private final int replicaCount;
    private final long replicaOpTimeoutMs;
    private final ReplicaTimedOps timedOps;

    public ReplicatedEntityHandler(
            List<ReplicaStore> replicas,
            List<AtomicBoolean> replicaEnabled,
            int replicaCount,
            ExecutorService replicaOpExecutor,
            long replicaOpTimeoutMs) {
        this.replicaEnabled = List.copyOf(replicaEnabled);
        this.replicaCount = replicaCount;
        this.replicaOpTimeoutMs = replicaOpTimeoutMs;
        this.timedOps =
                new ReplicaTimedOps(replicas, this.replicaEnabled, replicaOpExecutor, replicaOpTimeoutMs, log);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (HttpExchange ex = exchange) {
            try {
                String query = ex.getRequestURI().getQuery();
                String id = EntityQueryUtils.parseEntityIdFromQuery(query);
                int ack = resolveAck(query);
                if (ack <= 0 || ack > replicaCount) {
                    replyWithoutBody(ex, HttpCodes.BAD_REQUEST);
                    return;
                }
                String method = ex.getRequestMethod();
                switch (method) {
                    case "PUT" -> runPut(ex, id, ack);
                    case "GET" -> runGet(ex, id, ack);
                    case "DELETE" -> runDelete(ex, id, ack);
                    case null, default -> replyWithoutBody(ex, HttpCodes.METHOD_NOT_ALLOWED);
                }
            } catch (IllegalArgumentException ex2) {
                replyWithoutBody(ex, HttpCodes.BAD_REQUEST);
            } catch (Exception ex2) {
                log.error("entity endpoint: unexpected failure", ex2);
                replyWithoutBody(ex, HttpCodes.INTERNAL_ERROR);
            }
        }
    }

    private void runPut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] payload;
        try (InputStream in = exchange.getRequestBody()) {
            payload = in.readAllBytes();
        }
        long ts = System.nanoTime();
        log.debug("PUT key={} ack={} ts={} timeoutMs={}", id, ack, ts, replicaOpTimeoutMs);
        int successes = timedOps.upsertAll(id, payload, ts);
        boolean quorum = successes >= ack;
        log.info(
                "PUT quorum key={} ack={} successes={}/{} quorumMet={}",
                id,
                ack,
                successes,
                countEnabledReplicas(),
                quorum);
        replyWithoutBody(exchange, quorum ? HttpCodes.CREATED : HttpCodes.INTERNAL_ERROR);
    }

    private void runGet(HttpExchange exchange, String id, int ack) throws IOException {
        log.debug("GET key={} ack={} timeoutMs={}", id, ack, replicaOpTimeoutMs);
        ReplicaTimedOps.GetReadResult outcome = timedOps.getAll(id);
        boolean quorum = outcome.responses() >= ack;
        log.info(
                "GET quorum key={} ack={} responses={}/{} quorumMet={}",
                id,
                ack,
                outcome.responses(),
                countEnabledReplicas(),
                quorum);
        if (!quorum) {
            replyWithoutBody(exchange, HttpCodes.INTERNAL_ERROR);
            return;
        }
        VersionedValue best = outcome.best();
        if (best == null || best.isDeleted()) {
            replyWithoutBody(exchange, HttpCodes.NOT_FOUND);
            return;
        }
        byte[] data = best.getData();
        exchange.sendResponseHeaders(HttpCodes.OK, data.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(data);
        }
    }

    private void runDelete(HttpExchange exchange, String id, int ack) throws IOException {
        long ts = System.nanoTime();
        log.debug("DELETE key={} ack={} ts={} timeoutMs={}", id, ack, ts, replicaOpTimeoutMs);
        int successes = timedOps.deleteAll(id, ts);
        boolean quorum = successes >= ack;
        log.info(
                "DELETE quorum key={} ack={} successes={}/{} quorumMet={}",
                id,
                ack,
                successes,
                countEnabledReplicas(),
                quorum);
        replyWithoutBody(exchange, quorum ? HttpCodes.ACCEPTED : HttpCodes.INTERNAL_ERROR);
    }

    private int countEnabledReplicas() {
        int c = 0;
        for (int idx = 0; idx < replicaCount; idx++) {
            if (replicaEnabled.get(idx).get()) {
                c++;
            }
        }
        return c;
    }

    private int resolveAck(String query) {
        Integer parsed = EntityQueryUtils.parseAckFromQuery(query);
        return parsed == null ? 1 : parsed;
    }

    private static void replyWithoutBody(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, NO_BODY);
    }
}
