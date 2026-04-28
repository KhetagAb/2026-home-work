package company.vk.edu.distrib.compute.khetagab.replica;

import java.io.IOException;

interface ReplicaStore {

    VersionedValue get(String key) throws IOException;

    void upsert(String key, byte[] data, long timestamp) throws IOException;

    void delete(String key, long timestamp) throws IOException;
}
