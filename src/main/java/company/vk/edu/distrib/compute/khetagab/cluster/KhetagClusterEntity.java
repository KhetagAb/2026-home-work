package company.vk.edu.distrib.compute.khetagab.cluster;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.khetagab.EntityQueryUtils;
import company.vk.edu.distrib.compute.khetagab.HttpCodes;
import company.vk.edu.distrib.compute.khetagab.grpc.DeleteRequest;
import company.vk.edu.distrib.compute.khetagab.grpc.GetRequest;
import company.vk.edu.distrib.compute.khetagab.grpc.GetResponse;
import company.vk.edu.distrib.compute.khetagab.grpc.KvInternalServiceGrpc;
import company.vk.edu.distrib.compute.khetagab.grpc.PutRequest;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

class KhetagClusterEntity implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(KhetagClusterEntity.class);
    private static final int NO_BODY = -1;
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int PROXY_DEADLINE_SECONDS = 2;

    private final KhetagClusterNode node;

    KhetagClusterEntity(KhetagClusterNode node) {
        this.node = node;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (HttpExchange ex = exchange) {
            try {
                String method = ex.getRequestMethod();
                switch (method) {
                    case "PUT" -> runPut(ex);
                    case "GET" -> runGet(ex);
                    case "DELETE" -> runDelete(ex);
                    case null, default -> sendStatus(ex, HttpCodes.METHOD_NOT_ALLOWED);
                }
            } catch (IllegalArgumentException e) {
                sendStatus(ex, HttpCodes.BAD_REQUEST);
            } catch (Exception e) {
                log.error("cluster entity endpoint: unexpected failure", e);
                sendStatus(ex, HttpCodes.INTERNAL_ERROR);
            }
        }
    }

    private void runPut(HttpExchange ex) throws IOException {
        String key = EntityQueryUtils.parseEntityIdFromQuery(ex.getRequestURI().getQuery());
        byte[] payload;
        try (InputStream in = ex.getRequestBody()) {
            payload = in.readAllBytes();
        }
        if (node.isLocalOwner(key)) {
            node.localPut(key, payload);
            sendStatus(ex, HttpCodes.CREATED);
            return;
        }
        try {
            PutRequest putRequest = PutRequest.newBuilder().
                    setKey(key).
                    setValue(ByteString.copyFrom(payload)).
                    build();
            blockingStub(key).put(putRequest);
            sendStatus(ex, HttpCodes.CREATED);
        } catch (StatusRuntimeException sre) {
            log.warn("proxy PUT failed for key {}: {}", key, sre.getStatus());
            sendStatus(ex, SERVICE_UNAVAILABLE);
        }
    }

    private void runGet(HttpExchange ex) throws IOException {
        String key = EntityQueryUtils.parseEntityIdFromQuery(ex.getRequestURI().getQuery());
        if (node.isLocalOwner(key)) {
            try {
                byte[] value = node.localGet(key);
                ex.sendResponseHeaders(HttpCodes.OK, value.length);
                try (OutputStream out = ex.getResponseBody()) {
                    out.write(value);
                }
            } catch (NoSuchElementException e) {
                sendStatus(ex, HttpCodes.NOT_FOUND);
            }
            return;
        }
        try {
            GetRequest getRequest = GetRequest.newBuilder().
                    setKey(key).
                    build();
            GetResponse resp = blockingStub(key).get(getRequest);
            if (!resp.getFound()) {
                sendStatus(ex, HttpCodes.NOT_FOUND);
                return;
            }
            byte[] value = resp.getValue().toByteArray();
            ex.sendResponseHeaders(HttpCodes.OK, value.length);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(value);
            }
        } catch (StatusRuntimeException sre) {
            log.warn("proxy GET failed for key {}: {}", key, sre.getStatus());
            sendStatus(ex, SERVICE_UNAVAILABLE);
        }
    }

    private void runDelete(HttpExchange ex) throws IOException {
        String key = EntityQueryUtils.parseEntityIdFromQuery(ex.getRequestURI().getQuery());
        if (node.isLocalOwner(key)) {
            node.localDelete(key);
            sendStatus(ex, HttpCodes.ACCEPTED);
            return;
        }
        try {
            DeleteRequest deleteRequest = DeleteRequest.newBuilder().
                    setKey(key).
                    build();
            blockingStub(key).delete(deleteRequest);
            sendStatus(ex, HttpCodes.ACCEPTED);
        } catch (StatusRuntimeException sre) {
            log.warn("proxy DELETE failed for key {}: {}", key, sre.getStatus());
            sendStatus(ex, SERVICE_UNAVAILABLE);
        }
    }

    private KvInternalServiceGrpc.KvInternalServiceBlockingStub blockingStub(String key) {
        return KvInternalServiceGrpc.newBlockingStub(node.channelToOwner(key))
            .withDeadlineAfter(PROXY_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static void sendStatus(HttpExchange ex, int status) throws IOException {
        ex.sendResponseHeaders(status, NO_BODY);
    }
}
