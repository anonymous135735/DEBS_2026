package ch.usi.da.paxos;

import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.api.PaxosRole;
import org.apache.zookeeper.ZooKeeper;
import ch.usi.da.paxos.MRPUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MRPAcceptor {
    private static final String UNIQUE_ID = java.util.UUID.randomUUID().toString().substring(0, 8);
    private static int nodeId;
    private static List<RingDescription> rings = new ArrayList<>();
    private static Node node;

    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("MRPAcceptorID: " + UNIQUE_ID);

        nodeId = Integer.parseInt(args[0]);
        System.out.println("Node ID: " + nodeId);
      
        try {
            MRPUtils.loadMRPArgs();
        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }           
        
        try {
            initAcceptor();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down MRPAcceptor...");
                shutdown();
            }));

            Thread.currentThread().join();
            
        } catch (Exception e) {
            System.err.println("MRPAcceptor failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void initAcceptor() {
        System.out.println("Initializing MRP_acceptor ...");
        
        try {
            List<Integer> ringIds = new ArrayList<>();
            for (int i = 0; i < MRPUtils.RING_COUNT; i++) {
                ringIds.add(i);
            }

            for (int ringId : ringIds) {
                List<PaxosRole> roles = new ArrayList<>();
                roles.add(PaxosRole.Acceptor);
                rings.add(new RingDescription(ringId, roles));
            }
            // Create Paxos Node
            node = new Node(nodeId, MRPUtils.GROUP_ID, MRPUtils.ZK_SERVERS, rings);
            
            node.start();

            System.out.println("MRPAcceptor started successfully on node " + nodeId);
            System.out.println("Participating in rings: " + ringIds);
            
        } catch (Exception e) {
            System.err.println("Error while initializing MRP acceptor: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void shutdown() {
        try {
            if (node != null) {
                node.stop();
            }
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }
}