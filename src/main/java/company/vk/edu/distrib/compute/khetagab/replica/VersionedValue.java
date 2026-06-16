package company.vk.edu.distrib.compute.khetagab.replica;

import java.util.Arrays;

public final class VersionedValue {

    private final byte[] data;
    private final long timestamp;
    private final boolean deleted;

    private VersionedValue(byte[] data, long timestamp, boolean deleted) {
        this.data = data;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    static VersionedValue of(byte[] data, long timestamp) {
        return new VersionedValue(data, timestamp, false);
    }

    static VersionedValue tombstone(long timestamp) {
        return new VersionedValue(null, timestamp, true);
    }

    public byte[] getData() {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }

    long getTimestamp() {
        return timestamp;
    }

    public boolean isDeleted() {
        return deleted;
    }
}
