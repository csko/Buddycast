package buddycast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
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
    private final int protocolID;
    private String prefix;

    public RecommendationInitializer(String prefix) {
        protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        this.prefix = prefix;
    }

    public boolean execute() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("test/ua_norm.base"));
            String line;
            String[] split;
            int i;
            int itemID;
            Deque<Integer> prefs = new ArrayDeque<Integer>();
            ArrayList<Integer> nodes = new ArrayList<Integer>();
            while ((line = br.readLine()) != null) {
                split = line.split("\t");
                i = Integer.parseInt(split[0]) - 1;
                nodes.add(i);
                itemID = Integer.parseInt(split[1]);
                BuddyCast bc = (BuddyCast) Network.get(i).getProtocol(protocolID);
                prefs.clear();
                prefs.add(itemID);
                bc.addMyPreferences(prefs);
                bc.setInit(true);
                EDSimulator.add(0, BuddyCast.CycleMessage.getInstance(), Network.get(i), protocolID);
            }
            /* Set the topology */
            WireKOut wire = new WireKOut(prefix);
            wire.execute();
            for(Integer node : nodes){
                BuddyCast bc = (BuddyCast) Network.get(node).getProtocol(protocolID);
                bc.setInit(false);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
