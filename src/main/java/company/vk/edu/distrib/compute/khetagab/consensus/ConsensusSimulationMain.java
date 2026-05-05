package company.vk.edu.distrib.compute.khetagab.consensus;

import java.util.Scanner;

@SuppressWarnings("PMD.SystemPrintln")
public final class ConsensusSimulationMain {

    public static void main(String[] args) {
        try {
            start(args);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private static void start(String... args) throws InterruptedException {
        int nodeCount = 5;
        if (args.length > 0) {
            try {
                nodeCount = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("bad node count, using 5");
            }
        }
        ConsensusCluster cluster = new ConsensusCluster(nodeCount, ConsensusConfig.defaults());
        cluster.start();
        print(cluster);
        System.out.println("commands: up <id>  down <id>  p  q");

        Scanner in = new Scanner(System.in);
        while (in.hasNextLine()) {
            String[] a = in.nextLine().trim().split("\\s+");
            if (a.length == 0 || a[0].isEmpty()) {
                continue;
            }
            String cmd = a[0];
            if ("q".equalsIgnoreCase(cmd)) {
                break;
            }
            if ("p".equalsIgnoreCase(cmd)) {
                print(cluster);
                continue;
            }
            if (a.length < 2) {
                System.out.println("need id, e.g. up 3");
                continue;
            }
            int id;
            try {
                id = Integer.parseInt(a[1]);
            } catch (NumberFormatException ex) {
                System.out.println("not a number");
                continue;
            }
            if (id < 1 || id > nodeCount) {
                System.out.println("id 1.." + nodeCount);
                continue;
            }
            if ("up".equalsIgnoreCase(cmd)) {
                cluster.setNodeEnabled(id, true);
            } else if ("down".equalsIgnoreCase(cmd)) {
                cluster.setNodeEnabled(id, false);
            } else {
                System.out.println("unknown (up/down/p/q)");
                continue;
            }
            print(cluster);
        }
        cluster.stop();
    }

    private static void print(ConsensusCluster cluster) {
        for (ConsensusNode n : cluster.nodesView()) {
            System.out.println(n.getId() + " leader=" + n.getLeaderId() + " " + (n.isEnabled() ? "up" : "down"));
        }
    }

    private ConsensusSimulationMain() {
    }
}
