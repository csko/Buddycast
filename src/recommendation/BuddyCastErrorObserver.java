package recommendation;

import buddycast.BuddyCast;
import java.util.Map;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.reports.GraphObserver;

public class BuddyCastErrorObserver extends GraphObserver {
    //------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------

    private static final String PAR_APROT = "appprotocol";
    //------------------------------------------------------------------------
    // Fields
    //------------------------------------------------------------------------
    private final int apid;
    private double mae = 1.0;

    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    public BuddyCastErrorObserver(String prefix) {
        super(prefix);
        apid = Configuration.getPid(prefix + "." + PAR_APROT);
    }

    //------------------------------------------------------------------------
    // Methods
    //------------------------------------------------------------------------
    public boolean execute() {
        boolean ret = false;

        try {
            // update changes
            updateGraph();

            mae = 0.0;
            double numOfPreds = 0.0;
            //Map<Long, Integer> inDegrees = new TreeMap<Long, Integer>();
            int maxSelection = 0;

            for (int i = 0; i < g.size(); i++) {
                // get the current node
                Node currentNode = (Node) g.getNode(i);
                BuddyCast overlay = (BuddyCast) currentNode.getProtocol(pid);
                CoFeMethod userNode = (CoFeMethod) currentNode.getProtocol(apid);

                // get user id
                //long userID = userNode.getNode().getID();

                // get test data
                Map<Integer, Double> expecteds = userNode.getExpectedRates();
                Map<Integer, Double> predictions = userNode.getPredictedRates();

                // compute error
                for (int itemID : expecteds.keySet()) {
                    double expected = expecteds.get(itemID);
                    Double predicted = predictions.get(itemID);

                    //System.err.println(userID + "\t" + itemID + "\t" + expected + "\t" + predicted + "\t" + predictions.size());

                    mae += Math.abs(expected - predicted);
                    numOfPreds++;
                }

                // compute frequencies
        /*for (int n = 0; n < overlay.getCacheSize(); n ++) {
                if (! inDegrees.containsKey(overlay.getNeighbor(n).getID())) {
                inDegrees.put(overlay.getNeighbor(n).getID(), 0);
                }
                inDegrees.put(overlay.getNeighbor(n).getID(), inDegrees.get(overlay.getNeighbor(n).getID()) + 1);
                }*/

                // compute max selection and handle selection counters
                //if (overlay.getSelectionCounter() > maxSelection) {
//          maxSelection = overlay.getSelectionCounter();
//        }
//        overlay.initSelectionCounter();
            }
            // normalize error
            mae /= numOfPreds;

            // get max degree and degree distribution
      /*int maxInDegree = 0;
            Map<Integer, Integer> degreeDist = new TreeMap<Integer, Integer>();
            for (long nodeID : inDegrees.keySet()) {
            Integer inDegree = inDegrees.get(nodeID);
            // compute maximum of frequencies
            if (inDegree > maxInDegree) {
            maxInDegree = inDegree;
            }
            // compute degree distribution
            if (!degreeDist.containsKey(inDegree)) {
            degreeDist.put(inDegree, 0);
            }
            degreeDist.put(inDegree, degreeDist.get(inDegree) + 1);
            }*/

            // print MAE
            System.out.println("MAE= " + mae + " maxSelection= " + maxSelection);

            // print degree distribution
            //for (int inDegree : degreeDist.keySet()) {
            //  int freq = degreeDist.get(inDegree);
            //  System.out.println(inDegree + "\t" + freq);
            //}

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            ret = true;
        }
        return ret;
    }
}
