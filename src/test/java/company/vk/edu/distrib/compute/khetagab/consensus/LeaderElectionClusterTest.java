package company.vk.edu.distrib.compute.khetagab.consensus;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LeaderElectionClusterTest {

    private static final long AWAIT_SEC = 15L;

    private static long deadlineNanos() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SEC);
    }

    @Test
    void electsMaxOnStart() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(5, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            assertEquals(5, cluster.node(1).getLeaderId());
            assertEquals(5, cluster.node(3).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    @Test
    void singleNodeLeader() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(1, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            assertEquals(1, cluster.node(1).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    @Test
    void electsNextAfterLeaderDown() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(5, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            cluster.setNodeEnabled(5, false);
            awaitConsistentLeader(cluster, deadlineNanos());
            assertEquals(4, cluster.maxEnabledId());
            assertEquals(4, cluster.node(1).getLeaderId());
            assertEquals(4, cluster.node(4).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    @Test
    void highestIdReturnsAsLeader() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(5, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            cluster.setNodeEnabled(5, false);
            awaitConsistentLeader(cluster, deadlineNanos());
            cluster.setNodeEnabled(5, true);
            awaitConsistentLeader(cluster, deadlineNanos());
            assertEquals(5, cluster.maxEnabledId());
            assertEquals(5, cluster.node(2).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    @Test
    void staysConsistentOnFlaps() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(5, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            for (int round = 0; round < 6; round++) {
                cluster.setNodeEnabled(2, false);
                awaitConsistentLeader(cluster, deadlineNanos());
                cluster.setNodeEnabled(2, true);
                awaitConsistentLeader(cluster, deadlineNanos());
                cluster.setNodeEnabled(5, false);
                awaitConsistentLeader(cluster, deadlineNanos());
                cluster.setNodeEnabled(5, true);
                awaitConsistentLeader(cluster, deadlineNanos());
            }
            assertEquals(5, cluster.maxEnabledId());
            assertEquals(5, cluster.node(1).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    @Test
    void noStormAfterConverge() throws Exception {
        ConsensusCluster cluster = new ConsensusCluster(7, ConsensusConfig.forTests());
        try {
            cluster.start();
            awaitConsistentLeader(cluster, deadlineNanos());
            int before = totalElectionStarts(cluster);
            TimeUnit.MILLISECONDS.sleep(900L);
            awaitConsistentLeader(cluster, deadlineNanos());
            int after = totalElectionStarts(cluster);
            assertEquals(before, after);
            assertEquals(7, cluster.node(1).getLeaderId());
            assertEquals(7, cluster.node(6).getLeaderId());
        } finally {
            cluster.stop();
        }
    }

    private static int totalElectionStarts(ConsensusCluster cluster) {
        int total = 0;
        for (ConsensusNode node : cluster.nodesView()) {
            total += node.getElectionsStarted();
        }
        return total;
    }

    private static void awaitConsistentLeader(
            ConsensusCluster cluster,
            long deadlineNanos
    ) throws InterruptedException {
        int maxId = cluster.maxEnabledId();
        while (System.nanoTime() < deadlineNanos) {
            if (maxId < 0) {
                TimeUnit.MILLISECONDS.sleep(5L);
                maxId = cluster.maxEnabledId();
                continue;
            }
            boolean ok = true;
            for (ConsensusNode node : cluster.nodesView()) {
                if (!node.isEnabled()) {
                    continue;
                }
                if (node.getLeaderId() != maxId) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(5L);
            maxId = cluster.maxEnabledId();
        }
        throw new AssertionError("leader did not converge before deadline");
    }
}
