package buddycast;

import java.util.List;
import java.util.PriorityQueue;
import peersim.core.Node;

/**
 *
 * @author csko
 */
public class BuddyCastMessage {

    /**
     * The most recent 50 preferences of the active peer
     */
    PriorityQueue myPrefs;
    /**
     * The list of taste buddies
     */
    List<TasteBuddy> tasteBuddies;
    /**
     * The list of random peers
     */
    List<Node> randomPeers;
    /**
     * True, if the client is connectible;
     */
    boolean connectible;
}
