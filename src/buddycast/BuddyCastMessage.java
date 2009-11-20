package buddycast;

import java.util.Deque;
import java.util.Hashtable;
import peersim.core.Node;

/**
 *
 * @author csko
 */
class BuddyCastMessage {

    public BuddyCastMessage() {
    }
    /**
     * The sender Node.
     */
    Node sender;
    /**
     * The most recent 50 preferences of the active peer
     */
    Deque<Integer> myPrefs;
    /**
     * The list of taste buddies
     */
    Hashtable<Node, TasteBuddy> tasteBuddies; // peerID, TB(prefs, lastSeen)
    /**
     * The list of random peers
     */
    Hashtable<Node, Long> randomPeers; // peerID, lastSeen
    /**
     * True if and only if the client is connectible.
     */
    boolean connectible;
    /**
     * True if and only if this is a reply to a previous message.
     */
    boolean reply;
}
