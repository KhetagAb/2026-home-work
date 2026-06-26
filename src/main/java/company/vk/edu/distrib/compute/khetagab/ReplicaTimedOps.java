package company.vk.edu.distrib.compute.khetagab;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class ReplicaTimedOps {

    private final List<ReplicaStore> replicas;
    private final List<AtomicBoolean> replicaEnabled;
    private final ExecutorService executor;
    private final long timeoutMs;
    private final Logger log;

    ReplicaTimedOps(
            List<ReplicaStore> replicas,
            List<AtomicBoolean> replicaEnabled,
            ExecutorService executor,
            long timeoutMs,
            Logger log) {
        this.replicas = List.copyOf(replicas);
        this.replicaEnabled = List.copyOf(replicaEnabled);
        this.executor = executor;
        this.timeoutMs = timeoutMs;
        this.log = log;
    }

    int upsertAll(String id, byte[] payload, long ts) throws IOException {
        return runWrites("PUT", id, store -> store.upsert(id, payload, ts));
    }

    int deleteAll(String id, long ts) throws IOException {
        return runWrites("DELETE", id, store -> store.delete(id, ts));
    }

    GetReadResult getAll(String id) throws IOException {
        int responses = 0;
        VersionedValue best = null;
        int n = replicas.size();
        for (int i = 0; i < n; i++) {
            if (!replicaEnabled.get(i).get()) {
                continue;
            }
            int idx = i;
            Future<VersionedValue> f = executor.submit(() -> replicas.get(idx).get(id));
            try {
                VersionedValue vv = f.get(timeoutMs, TimeUnit.MILLISECONDS);
                responses++;
                best = newer(best, vv);
            } catch (TimeoutException e) {
                f.cancel(true);
                log.warn("GET timeout key={} replica={}", id, idx);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.cancel(true);
                log.warn("GET interrupted key={} replica={}", id, idx);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }
        return new GetReadResult(responses, best);
    }

    @FunctionalInterface
    private interface WriteOp {
        void run(ReplicaStore store) throws IOException;
    }

    private int runWrites(String op, String id, WriteOp opOnStore) throws IOException {
        int ok = 0;
        int n = replicas.size();
        for (int i = 0; i < n; i++) {
            if (!replicaEnabled.get(i).get()) {
                continue;
            }
            int idx = i;
            Future<Void> f =
                    executor.submit(
                            () -> {
                                opOnStore.run(replicas.get(idx));
                                return null;
                            });
            try {
                f.get(timeoutMs, TimeUnit.MILLISECONDS);
                ok++;
            } catch (TimeoutException e) {
                f.cancel(true);
                log.warn("{} timeout key={} replica={}", op, id, idx);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.cancel(true);
                log.warn("{} interrupted key={} replica={}", op, id, idx);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }
        return ok;
    }

    private static VersionedValue newer(VersionedValue best, VersionedValue v) {
        if (v == null) {
            return best;
        }
        if (best == null || v.getTimestamp() > best.getTimestamp()) {
            return v;
        }
        return best;
    }

    record GetReadResult(int responses, VersionedValue best) {}
}
