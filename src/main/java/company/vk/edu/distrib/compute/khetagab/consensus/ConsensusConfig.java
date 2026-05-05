package company.vk.edu.distrib.compute.khetagab.consensus;

import java.util.Random;

public record ConsensusConfig(long pollTimeoutMs, long electionWaitMs, long pingIntervalMs, long pingTimeoutMs,
                              double failureProbabilityPerPoll, Random random) {

    public static ConsensusConfig forTests() {
        return new ConsensusConfig(25L, 400L, 70L, 320L, 0.0, new Random(0L));
    }

    public static ConsensusConfig defaults() {
        return new ConsensusConfig(200L, 500L, 400L, 1200L, 0.001, new Random());
    }
}
