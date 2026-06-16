package company.vk.edu.distrib.compute.khetagab.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class KhetagCluster implements KVCluster {

    private static final Logger log = LoggerFactory.getLogger(KhetagCluster.class);

    private final Map<String, KhetagClusterNode> nodesByEndpoint;
    private final List<String> endpoints;

    KhetagCluster(List<Integer> ports) {
        List<NodeInfo> allNodes = new ArrayList<>(ports.size());
        for (int httpPort : ports) {
            int grpcPort = findFreePort();
            allNodes.add(new NodeInfo(httpPort, grpcPort));
        }
        Map<String, KhetagClusterNode> byEndpoint = new ConcurrentHashMap<>();
        List<String> endpointList = new ArrayList<>(ports.size());
        registerNodesWithEndpoints(allNodes, byEndpoint, endpointList);
        this.nodesByEndpoint = Map.copyOf(byEndpoint);
        this.endpoints = List.copyOf(endpointList);
    }

    @Override
    public void start() {
        for (KhetagClusterNode node : nodesByEndpoint.values()) {
            startNode(node);
        }
    }

    @Override
    public void start(String endpoint) {
        KhetagClusterNode node = nodesByEndpoint.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        startNode(node);
    }

    @Override
    public void stop() {
        for (KhetagClusterNode node : nodesByEndpoint.values()) {
            stopNode(node);
        }
    }

    @Override
    public void stop(String endpoint) {
        KhetagClusterNode node = nodesByEndpoint.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        stopNode(node);
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private static void startNode(KhetagClusterNode node) {
        try {
            node.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start cluster node", ex);
        }
    }

    private static void stopNode(KhetagClusterNode node) {
        try {
            node.stop();
        } catch (Exception ex) {
            log.warn("Error stopping cluster node", ex);
        }
    }

    private static int findFreePort() {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot allocate free port", ex);
        }
    }

    private static void registerNodesWithEndpoints(
            List<NodeInfo> allNodes,
            Map<String, KhetagClusterNode> nodesByEndpoint,
            List<String> orderedEndpoints) {
        for (NodeInfo info : allNodes) {
            // заглушка для локальной версии
            String endpoint = "http://localhost:" + info.httpPort();
            KhetagClusterNode node = new KhetagClusterNode(info.httpPort(), info.grpcPort(), allNodes);
            nodesByEndpoint.put(endpoint, node);
            orderedEndpoints.add(endpoint);
        }
    }
}
