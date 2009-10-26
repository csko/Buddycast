package buddycast;

import buddycast.BuddyCast.CycleMessage;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.edsim.EDSimulator;

/**
 *
 * @author csko
 */
public class BuddyCastInitializer implements Control {

    /**
     * Protocol ID to send events to.
     * @config
     */
    private static final String PAR_PROT = "protocol";
    private final int protocolID;

    public BuddyCastInitializer(String prefix) {
        protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    public boolean execute() {
        assert (BuddyCast.numSuperPeers >= Network.size());
        /* First, Super Peers should act */
        for (int i = 0; i < BuddyCast.numSuperPeers; i++) {
            EDSimulator.add(0, BuddyCast.CycleMessage.getInstance(),
                    Network.get(i), protocolID);
        }
        /* Then all the other peers should act in random order */
        for (int i = BuddyCast.numSuperPeers; i < Network.size(); i++) {
            EDSimulator.add(CommonState.r.nextLong(BuddyCast.timeToWait + 1),
                    CycleMessage.getInstance(), Network.get(i), protocolID);
        }
        return false;
    }
}
