package buddycast;

import java.util.Deque;
import java.util.Hashtable;

/**
 *
 * @author csko
 */
public class BuddyCastMessage {

    public BuddyCastMessage() {
    }

    /**
     * The most recent 50 preferences of the active peer
     */
    Deque<Integer> myPrefs;
    /**
     * The list of taste buddies
     */
    Hashtable<Long, TasteBuddy> tasteBuddies; // peerID, TB(prefs, lastSeen)
    /**
     * The list of random peers
     */
    Hashtable<Long, Long> randomPeers; // peerID, lastSeen
    /**
     * True, if the client is connectible;
     */
    boolean connectible;
}
