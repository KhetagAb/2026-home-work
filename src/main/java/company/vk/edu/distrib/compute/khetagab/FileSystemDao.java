package company.vk.edu.distrib.compute.khetagab;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class FileSystemDao implements Dao<byte[]> {

    private final Path storageRoot;

    public FileSystemDao(Path storageRoot) throws IOException {
        this.storageRoot = Objects.requireNonNull(storageRoot).normalize();
        if (!Files.exists(storageRoot)) {
            Files.createDirectories(storageRoot);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            throw new NoSuchElementException(key);
        }
        Path file = resolveUnderRoot(key);
        if (!Files.isRegularFile(file)) {
            throw new NoSuchElementException(key);
        }
        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Objects.requireNonNull(value, "value");
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            throw new IllegalArgumentException("Invalid key");
        }
        Path file = resolveUnderRoot(key);
        Files.createDirectories(file.getParent());
        Files.write(file, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKeyPresent(key);
        if (isUnsafePathKey(key)) {
            return;
        }
        Path file = resolveUnderRoot(key);
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        if (!Files.exists(storageRoot)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(storageRoot)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            IOException first = null;
            for (Path path : paths) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }

    private static void validateKeyPresent(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key is null or empty");
        }
    }

    private static boolean isUnsafePathKey(String key) {
        return key.indexOf('/') >= 0 || key.indexOf('\\') >= 0 || key.contains("..");
    }

    private Path resolveUnderRoot(String key) {
        Path resolved = storageRoot.resolve(key).normalize();
        if (!resolved.startsWith(storageRoot)) {
            throw new IllegalArgumentException("Invalid key");
        }
        return resolved;
    }
}
