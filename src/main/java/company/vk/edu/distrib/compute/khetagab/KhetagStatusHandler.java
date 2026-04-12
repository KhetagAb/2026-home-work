package company.vk.edu.distrib.compute.khetagab;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

public final class KhetagStatusHandler implements HttpHandler {

    private static final int NO_BODY = -1;

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (HttpExchange ex = exchange) {
            if (!"GET".equals(ex.getRequestMethod())) {
                ex.sendResponseHeaders(HttpCodes.METHOD_NOT_ALLOWED, NO_BODY);
                return;
            }
            ex.sendResponseHeaders(HttpCodes.OK, NO_BODY);
        }
    }
}
