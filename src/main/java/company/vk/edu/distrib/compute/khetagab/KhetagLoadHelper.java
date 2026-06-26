package company.vk.edu.distrib.compute.khetagab;

import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KhetagLoadHelper {

    private static final Logger log = LoggerFactory.getLogger(KhetagLoadHelper.class);

    private KhetagLoadHelper() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        KVService storage = new KhetagKvServiceFactory().create(port);
        storage.start();
        log.info("KV server (khetagab) listening on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
        Thread.sleep(Long.MAX_VALUE);
    }
}
