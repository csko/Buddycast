package buddycast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayDeque;
import java.util.Deque;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
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

    public RecommendationInitializer(String prefix) {
        protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    public boolean execute() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("test/ua_norm.base"));
            String line;
            String[] split;
            int i;
            int itemID;
            Deque<Integer> prefs = new ArrayDeque<Integer>();
            while ((line = br.readLine()) != null) {
                split = line.split("\t");
                i = Integer.parseInt(split[0]) - 1;
                itemID = Integer.parseInt(split[1]);
                BuddyCast bc = (BuddyCast) Network.get(i).getProtocol(protocolID);
                prefs.clear();
                prefs.add(itemID);
                bc.addMyPreferences(prefs);
                EDSimulator.add(0, BuddyCast.CycleMessage.getInstance(), Network.get(i), protocolID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
