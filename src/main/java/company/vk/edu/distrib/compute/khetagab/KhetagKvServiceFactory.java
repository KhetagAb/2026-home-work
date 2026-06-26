package company.vk.edu.distrib.compute.khetagab;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class KhetagKvServiceFactory extends KVServiceFactory {

    private static final Path STORAGE_BASE = Path.of("khetag-ab-storage");

    @Override
    protected KVService doCreate(int port) throws IOException {
        Files.createDirectories(STORAGE_BASE);
        Path root = Files.createTempDirectory(STORAGE_BASE, "session-");
        Dao<byte[]> dao = new FileSystemDao(root);
        return new KVServiceImpl(port, dao);
    }
}
