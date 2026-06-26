package company.vk.edu.distrib.compute.khetagab;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

public final class KhetagEntityHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(KhetagEntityHandler.class);
    private static final int NO_BODY = -1;
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_DELETE = "DELETE";

    private final Dao<byte[]> dao;

    public KhetagEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (HttpExchange ex = exchange) {
            handleRequest(ex);
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        try {
            dispatchRequest(exchange);
        } catch (NoSuchElementException e) {
            replyWithoutBody(exchange, HttpCodes.NOT_FOUND);
        } catch (IllegalArgumentException e) {
            replyWithoutBody(exchange, HttpCodes.BAD_REQUEST);
        } catch (Exception e) {
            logUnexpectedFailure(e);
            replyWithoutBody(exchange, HttpCodes.INTERNAL_ERROR);
        }
    }

    private void dispatchRequest(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case METHOD_PUT -> runPut(exchange);
            case METHOD_GET -> runGet(exchange);
            case METHOD_DELETE -> runDelete(exchange);
            case null, default -> replyWithoutBody(exchange, HttpCodes.METHOD_NOT_ALLOWED);
        }
    }

    private static void logUnexpectedFailure(Exception e) {
        if (log.isErrorEnabled()) {
            log.error("entity endpoint: unexpected failure", e);
        }
    }

    private void runPut(HttpExchange exchange) throws IOException {
        String id = EntityQueryUtils.parseEntityIdFromQuery(exchange.getRequestURI().getQuery());
        byte[] payload;
        try (InputStream in = exchange.getRequestBody()) {
            payload = in.readAllBytes();
        }
        dao.upsert(id, payload);
        replyWithoutBody(exchange, HttpCodes.CREATED);
    }

    private void runGet(HttpExchange exchange) throws IOException {
        String id = EntityQueryUtils.parseEntityIdFromQuery(exchange.getRequestURI().getQuery());
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(HttpCodes.OK, value.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(value);
        }
    }

    private void runDelete(HttpExchange exchange) throws IOException {
        String id = EntityQueryUtils.parseEntityIdFromQuery(exchange.getRequestURI().getQuery());
        dao.delete(id);
        replyWithoutBody(exchange, HttpCodes.ACCEPTED);
    }

    private static void replyWithoutBody(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, NO_BODY);
    }
}
