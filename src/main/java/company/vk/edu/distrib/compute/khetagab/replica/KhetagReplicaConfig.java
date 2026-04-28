package company.vk.edu.distrib.compute.khetagab.replica;

final class KhetagReplicaConfig {

    static final String REPLICA_COUNT_ENV = "KHETAG_REPLICA_COUNT";
    static final String OP_TIMEOUT_MS_ENV = "KHETAG_REPLICA_OP_TIMEOUT_MS";

    private static final int DEFAULT_REPLICA_COUNT = 3;
    private static final long DEFAULT_OP_TIMEOUT_MS = 10_000L;

    private KhetagReplicaConfig() {
    }

    static int replicaCount() {
        String env = System.getenv(REPLICA_COUNT_ENV);
        if (env == null || env.isBlank()) {
            return DEFAULT_REPLICA_COUNT;
        }
        int v = Integer.parseInt(env.trim());
        return validateAtLeastThree(v);
    }

    static long replicaOpTimeoutMs() {
        String env = System.getenv(OP_TIMEOUT_MS_ENV);
        if (env == null || env.isBlank()) {
            return DEFAULT_OP_TIMEOUT_MS;
        }
        return validateTimeoutMs(Long.parseLong(env.trim()));
    }

    private static long validateTimeoutMs(long ms) {
        if (ms < 1L) {
            throw new IllegalArgumentException(
                    "Replica op timeout must be >= 1 ms; got " + ms + " from " + OP_TIMEOUT_MS_ENV);
        }
        return ms;
    }

    private static int validateAtLeastThree(int n) {
        if (n < 3) {
            throw new IllegalArgumentException(
                    "Replica count must be >= 3; got " + n + " from " + REPLICA_COUNT_ENV);
        }
        return n;
    }
}
