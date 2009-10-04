package buddycast;

import java.util.Deque;
import java.util.Hashtable;
import java.util.List;

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
    List<TasteBuddy> tasteBuddies;
    /**
     * The list of random peers
     */
    Hashtable<Integer, Long> randomPeers;
    /**
     * True, if the client is connectible;
     */
    boolean connectible;
}
