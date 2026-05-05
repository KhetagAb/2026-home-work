package company.vk.edu.distrib.compute.khetagab.consensus;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("PMD.SystemPrintln")
public final class ConsensusNode implements Runnable {
    private final int id;
    private final ConsensusCluster cluster;
    private final ConsensusConfig config;
    private final BlockingQueue<ConsensusMessage> inbox = new LinkedBlockingQueue<>();

    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicBoolean electionRequested = new AtomicBoolean(false);
    private final AtomicInteger electionsStarted = new AtomicInteger();

    private Thread worker;

    private long pingSentNano;
    private long lastLeaderOkNano;

    public ConsensusNode(int id, ConsensusCluster cluster, ConsensusConfig config) {
        this.id = id;
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.lastLeaderOkNano = System.nanoTime();
    }

    public int getId() {
        return id;
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    int getElectionsStarted() {
        return electionsStarted.get();
    }

    void setEnabled(boolean value) {
        boolean old = enabled.getAndSet(value);
        if (old != value) {
            System.out.println("node=" + id + " state " + (old ? "UP" : "DOWN") + " => " + (value ? "UP" : "DOWN"));
        }
    }

    void clearInbox() {
        inbox.clear();
    }

    void requestElection() {
        electionRequested.set(true);
    }

    void offer(ConsensusMessage message) {
        inbox.offer(message);
    }

    void startThread() {
        Thread t = new Thread(this, "consensus-node-" + id);
        this.worker = t;
        t.start();
    }

    void stopAndJoin() throws InterruptedException {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5L));
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                if (!enabled.get()) {
                    inbox.clear();
                    TimeUnit.MILLISECONDS.sleep(config.pollTimeoutMs());
                    continue;
                }
                maybeRandomFailure();
                if (electionRequested.getAndSet(false)) {
                    runElectionAsCandidate();
                }
                ConsensusMessage m = inbox.poll(config.pollTimeoutMs(), TimeUnit.MILLISECONDS);
                if (m != null) {
                    handleOutsideElection(m);
                }
                pingLeaderOrElect();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void maybeRandomFailure() {
        double p = config.failureProbabilityPerPoll();
        if (p > 0.0 && config.random().nextDouble() < p) {
            System.out.println("node=" + id + " random failure triggered");
            setEnabled(false);
        }
    }

    private void runElectionAsCandidate() throws InterruptedException {
        if (!enabled.get()) {
            return;
        }
        clearLeaderPingAwait();
        electionRequested.set(false);
        int round = electionsStarted.incrementAndGet();
        System.out.println("node=" + id + " starts election #" + round);
        for (int j = id + 1; j <= cluster.size(); j++) {
            cluster.deliver(j, ConsensusMessage.Kind.ELECT, id);
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.electionWaitMs());
        boolean suppressed = pollElectionWindow(deadlineNanos);
        if (suppressed) {
            System.out.println("node=" + id + " election suppressed by higher node");
        } else if (enabled.get()) {
            declareVictory();
        }
    }

    private boolean pollElectionWindow(long deadlineNanos) throws InterruptedException {
        while (enabled.get() && running.get()) {
            long now = System.nanoTime();
            if (now >= deadlineNanos) {
                break;
            }
            long waitMs = Math.min(
                    config.pollTimeoutMs(),
                    Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - now))
            );
            ConsensusMessage m = inbox.poll(waitMs, TimeUnit.MILLISECONDS);
            if (m != null && handleDuringElection(m)) {
                return true;
            }
        }
        return false;
    }

    private boolean handleDuringElection(ConsensusMessage m) {
        switch (m.kind()) {
            case ANSWER -> {
                if (m.senderId() > id) {
                    System.out.println("node=" + id + " got ANSWER from higher node=" + m.senderId()
                            + ", stops own campaign");
                    return true;
                }
            }
            case ELECT -> onElectFromLowerNode(m.senderId(), true);
            case PING -> {
                if (enabled.get() && leaderId.get() == id) {
                    cluster.deliver(m.senderId(), ConsensusMessage.Kind.ANSWER, id);
                }
            }
            case VICTORY -> {
                if (m.senderId() > id) {
                    applyVictory(m.senderId());
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    private void handleOutsideElection(ConsensusMessage m) {
        int currentLeader = leaderId.get();
        switch (m.kind()) {
            case ANSWER -> {
                if (m.senderId() == currentLeader && currentLeader != id) {
                    clearLeaderPingAwait();
                    lastLeaderOkNano = System.nanoTime();
                }
            }
            case ELECT -> onElectFromLowerNode(m.senderId(), false);
            case PING -> {
                if (enabled.get() && leaderId.get() == id) {
                    cluster.deliver(m.senderId(), ConsensusMessage.Kind.ANSWER, id);
                }
            }
            case VICTORY -> applyVictory(m.senderId());
        }
    }

    private void onElectFromLowerNode(int senderId, boolean inElectionWindow) {
        if (!enabled.get() || senderId >= id) {
            return;
        }
        String mode = inElectionWindow ? "starting own election" : "scheduling election";
        System.out.println("node=" + id + " got ELECT from node=" + senderId + ", answering and " + mode);
        cluster.deliver(senderId, ConsensusMessage.Kind.ANSWER, id);
        if (shouldScheduleElectionAfterElect()) {
            electionRequested.set(true);
        }
    }

    private boolean shouldScheduleElectionAfterElect() {
        int knownLeader = leaderId.get();
        return knownLeader != id && (knownLeader < 0 || knownLeader < id);
    }

    private void applyVictory(int newLeader) {
        int old = leaderId.getAndSet(newLeader);
        if (old != newLeader) {
            System.out.println("node=" + id + " accepts leader " + old + " => " + newLeader);
        }
        electionRequested.set(false);
        clearLeaderPingAwait();
        lastLeaderOkNano = System.nanoTime();
    }

    private void declareVictory() {
        System.out.println("node=" + id + " declares VICTORY");
        leaderId.set(id);
        electionRequested.set(false);
        clearLeaderPingAwait();
        cluster.broadcastVictory(id, id);
    }

    private void clearLeaderPingAwait() {
        pingSentNano = 0L;
    }

    private void pingLeaderOrElect() throws InterruptedException {
        int lid = leaderId.get();
        if (!enabled.get() || lid < 0) {
            return;
        }
        if (lid == id) {
            return;
        }
        long now = System.nanoTime();
        if (pingSentNano != 0L) {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - pingSentNano);
            if (elapsedMs >= config.pingTimeoutMs()) {
                System.out.println("node=" + id + " ping timeout to leader=" + lid + ", starts election");
                clearLeaderPingAwait();
                runElectionAsCandidate();
            }
            return;
        }
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(config.pingIntervalMs());
        if (now - lastLeaderOkNano >= intervalNanos) {
            cluster.deliver(lid, ConsensusMessage.Kind.PING, id);
            pingSentNano = now;
        }
    }
}
