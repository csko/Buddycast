package buddycast;

import peersim.core.*;
import peersim.edsim.EDProtocol;

/**
 * Event driven version of the BuddyCast protocol.
 */
public class BuddyCast
        implements EDProtocol {

    private String prefix;
/**
 * 
 * @param prefix
 */
    public BuddyCast(String prefix) {
        this.prefix = prefix;
    }

    /**
     * This is the standard method to define to process incoming messages.
     * @param node
     * @param pid
     * @param event
     */
    public void processEvent(Node node, int pid, Object event) {
    }

    /**
     *
     * @return
     */
    @Override
    public Object clone() {
        BuddyCast bc = null;
        try {
            bc = (BuddyCast) super.clone();
        } catch (CloneNotSupportedException e) {
        } // never happens
        return bc;
    }
}
