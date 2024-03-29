package recommendation;

import buddycast.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayDeque;
import java.util.Deque;

import java.util.HashSet;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.dynamics.WireKOut;
import peersim.edsim.EDSimulator;

/**
 *
 * @author csko
 */
public class RecommendationInitializer implements Control {

    /**
     * Protocol ID to send events to.
     * @config
     */
    private static final String PAR_PROT = "protocol";
    private static final String PAR_FILENAME = "filename";
    private final int protocolID;
    private String prefix;
    private final String fileName;

    public RecommendationInitializer(String prefix) {
        protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        fileName = Configuration.getString(prefix + "." + PAR_FILENAME);
        this.prefix = prefix;
    }

    public boolean execute() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line;
            String[] split;
            //int i;
            int itemID;
            //Deque<Integer> prefs = new ArrayDeque<Integer>();
            //HashSet<Integer> nodes = new HashSet<Integer>();
            for (int i = 0; i < Network.size(); i++) {
                BuddyCast bc = (BuddyCast) Network.get(i).getProtocol(protocolID);
                bc.setInit(true);
            }
            // Load the items
            /*
            while ((line = br.readLine()) != null) {
            split = line.split("\t");
            i = Integer.parseInt(split[0]) - 1;
            BuddyCast bc = (BuddyCast) Network.get(i).getProtocol(protocolID);
            if (!nodes.contains(i)) {
            nodes.add(i);
            bc.setInit(true);
            }
            itemID = Integer.parseInt(split[1]) - 1;
            prefs.clear();
            prefs.add(itemID);
            bc.addMyPreferences(prefs);
            }*/

            /* Set the topology */
            WireKOut wire = new WireKOut(prefix);
            wire.execute();
            for (int i = 0; i < Network.size(); i++) {
                Node node = Network.get(i);
                BuddyCast bc = (BuddyCast) Network.get(i).getProtocol(protocolID);
                EDSimulator.add(0, BuddyCast.CycleMessage.getInstance(), node, protocolID);
                bc.setInit(false);
            }
            // load similarities from a file
            //SimilarityMatrixFromFile sim = SimilarityMatrixFromFile.getInstance();
            //sim.computeSimilarity(0, 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
