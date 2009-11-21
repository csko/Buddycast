package recommendation;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.Protocol;

public class PredictionInitializer implements Control {
    //-----------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------

    private static final String PAR_PROT = "protocol";
    //------------------------------------------------------------------------
    // Fields
    //------------------------------------------------------------------------
    private final int pid;

    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    public PredictionInitializer(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    //------------------------------------------------------------------------
    // Methods
    //------------------------------------------------------------------------
    public boolean execute() {
        // init desriptor lists
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            Protocol prot = node.getProtocol(pid);
            if (prot instanceof CoFeMethod) {
                CoFeMethod method = (CoFeMethod) prot;
                method.initializePredictions();
            } else {
                return true;
            }
        }
        return false;
    }
}
