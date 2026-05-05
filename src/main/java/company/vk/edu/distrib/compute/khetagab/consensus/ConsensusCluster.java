package company.vk.edu.distrib.compute.khetagab.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("PMD.SystemPrintln")
public final class ConsensusCluster {
    private final List<ConsensusNode> nodes;

    public ConsensusCluster(int nodeCount, ConsensusConfig config) {
        this.nodes = new ArrayList<>(nodeCount);
        for (int id = 1; id <= nodeCount; id++) {
            nodes.add(new ConsensusNode(id, this, config));
        }
    }

    public int size() {
        return nodes.size();
    }

    public ConsensusNode node(int id) {
        return nodes.get(id - 1);
    }

    public List<ConsensusNode> nodesView() {
        return Collections.unmodifiableList(nodes);
    }

    public void setNodeEnabled(int id, boolean enabled) {
        ConsensusNode n = node(id);
        n.setEnabled(enabled);
        System.out.println("cluster force node=" + id + " => " + (enabled ? "UP" : "DOWN"));
        if (enabled) {
            n.clearInbox();
            n.requestElection();
        }
    }

    public void start() {
        System.out.println("cluster start, nodes=" + nodes.size());
        for (ConsensusNode n : nodes) {
            n.startThread();
        }
        if (!nodes.isEmpty()) {
            System.out.println("cluster bootstrap election via node=" + nodes.getFirst().getId());
            nodes.getFirst().requestElection();
        }
    }

    public void stop() throws InterruptedException {
        for (ConsensusNode n : nodes) {
            n.stopAndJoin();
        }
    }

    void deliver(int toId, ConsensusMessage message) {
        if (!node(toId).isEnabled()) {
            return;
        }
        node(toId).offer(message);
    }

    void deliver(int toId, ConsensusMessage.Kind kind, int senderId) {
        deliver(toId, new ConsensusMessage(kind, senderId));
    }

    void broadcastVictory(int fromId, int senderId) {
        for (ConsensusNode n : nodes) {
            if (n.getId() != fromId && n.isEnabled()) {
                n.offer(new ConsensusMessage(ConsensusMessage.Kind.VICTORY, senderId));
            }
        }
    }

    int maxEnabledId() {
        int max = -1;
        for (ConsensusNode n : nodes) {
            if (n.isEnabled() && n.getId() > max) {
                max = n.getId();
            }
        }
        return max;
    }
}
