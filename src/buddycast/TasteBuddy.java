package buddycast;

import java.util.Deque;
import peersim.core.Node;

/**
 *
 * @author csko
 */
class TasteBuddy {

    private Node node;
    private Deque<Integer> prefs;
    private Long lastSeen;

    public Long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(Long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node peerID) {
        this.node = peerID;
    }

    public Deque<Integer> getPrefs() {
        return prefs;
    }

    public void setPrefs(Deque<Integer> prefs) {
        this.prefs = prefs;
    }
}
