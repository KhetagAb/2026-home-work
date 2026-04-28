package company.vk.edu.distrib.compute.khetagab.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.khetagab.HttpCodes;

import java.io.IOException;

public final class KhetagStatus implements HttpHandler {

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
