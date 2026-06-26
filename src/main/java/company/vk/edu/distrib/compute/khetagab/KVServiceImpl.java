package company.vk.edu.distrib.compute.khetagab;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new KhetagStatusHandler());
        server.createContext("/v0/entity", new KhetagEntityHandler(dao));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("could not release filesystem storage cleanly", e);
            }
        }
    }
}
