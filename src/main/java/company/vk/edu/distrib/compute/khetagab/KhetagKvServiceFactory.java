package company.vk.edu.distrib.compute.khetagab;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.khetagab.replica.KhetagReplicatedService;

import java.io.IOException;

public class KhetagKvServiceFactory extends KVServiceFactory {

    //    private static final Path STORAGE_BASE = Path.of("khetag-ab-storage");
    //
    //    protected KVService doTask1Create(int port) throws IOException {
    //        Files.createDirectories(STORAGE_BASE);
    //        Path root = Files.createTempDirectory(STORAGE_BASE, "session-");
    //        Dao<byte[]> dao = new FileSystemDao(root);
    //        return new KVServiceImpl(port, dao);
    //    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KhetagReplicatedService(port);
    }
}
