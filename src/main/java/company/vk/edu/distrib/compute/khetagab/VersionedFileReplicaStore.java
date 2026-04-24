package company.vk.edu.distrib.compute.khetagab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

final class VersionedFileReplicaStore implements ReplicaStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VersionedFileReplicaStore.class);
    private static final String VALUE_SUFFIX = ".v";
    private static final String TOMB_SUFFIX = ".t";

    private final Path storageRoot;
    private final ReentrantLock lock = new ReentrantLock();

    VersionedFileReplicaStore(Path storageRoot) throws IOException {
        this.storageRoot = Objects.requireNonNull(storageRoot).normalize();
        if (!Files.exists(this.storageRoot)) {
            Files.createDirectories(this.storageRoot);
        }
    }

    @Override
    public VersionedValue get(String key) throws IOException {
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            return null;
        }
        Path dir = resolveKeyDir(key);
        lock.lock();
        try {
            if (!Files.isDirectory(dir)) {
                return null;
            }
            return readLatest(dir);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] data, long timestamp) throws IOException {
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            throw new IllegalArgumentException("Invalid key");
        }
        Path dir = resolveKeyDir(key);
        lock.lock();
        try {
            writeAtomicFile(dir, timestamp + VALUE_SUFFIX, data == null ? new byte[0] : data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key, long timestamp) throws IOException {
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            return;
        }
        Path dir = resolveKeyDir(key);
        lock.lock();
        try {
            writeAtomicFile(dir, timestamp + TOMB_SUFFIX, new byte[0]);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!Files.exists(storageRoot)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(storageRoot)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private void writeAtomicFile(Path keyDir, String fileName, byte[] body) throws IOException {
        Files.createDirectories(keyDir);
        Path target = keyDir.resolve(fileName);
        Path tmp = Files.createTempFile(keyDir, "w", ".tmp");
        try {
            Files.write(tmp, body, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException e) {
                log.debug("temp file cleanup failed: {}", tmp, e);
            }
        }
    }

    private static VersionedValue readLatest(Path keyDir) throws IOException {
        long bestTs = Long.MIN_VALUE;
        boolean tomb = false;
        Path valuePath = null;
        try (Stream<Path> stream = Files.list(keyDir)) {
            for (Path p : stream.toList()) {
                if (!Files.isRegularFile(p)) {
                    continue;
                }
                String name = p.getFileName().toString();
                Parsed parsed = parseVersionFileName(name);
                if (parsed == null) {
                    continue;
                }
                if (parsed.timestamp > bestTs) {
                    bestTs = parsed.timestamp;
                    tomb = parsed.tombstone;
                    valuePath = tomb ? null : p;
                } else if (parsed.timestamp == bestTs && parsed.tombstone) {
                    tomb = true;
                    valuePath = null;
                }
            }
        }
        if (bestTs == Long.MIN_VALUE) {
            return null;
        }
        if (tomb) {
            return VersionedValue.tombstone(bestTs);
        }
        byte[] raw = Files.readAllBytes(valuePath);
        return VersionedValue.of(raw, bestTs);
    }

    private record Parsed(long timestamp, boolean tombstone) {
    }

    private static Parsed parseVersionFileName(String name) {
        if (name.endsWith(VALUE_SUFFIX)) {
            String prefix = name.substring(0, name.length() - VALUE_SUFFIX.length());
            return new Parsed(Long.parseLong(prefix), false);
        }
        if (name.endsWith(TOMB_SUFFIX)) {
            String prefix = name.substring(0, name.length() - TOMB_SUFFIX.length());
            return new Parsed(Long.parseLong(prefix), true);
        }
        return null;
    }

    private static void validateKeyPresent(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key is null or empty");
        }
    }

    private static boolean isUnsafePathKey(String key) {
        return key.indexOf('/') >= 0 || key.indexOf('\\') >= 0 || key.contains("..");
    }

    private Path resolveKeyDir(String key) {
        Path resolved = storageRoot.resolve(key).normalize();
        if (!resolved.startsWith(storageRoot)) {
            throw new IllegalArgumentException("Invalid key");
        }
        return resolved;
    }
}
