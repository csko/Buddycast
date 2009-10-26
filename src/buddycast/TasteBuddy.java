package buddycast;

import java.util.Deque;

/**
 *
 * @author csko
 */
class TasteBuddy {

    private Long peerID;
    private Deque<Integer> prefs;
    private Long lastSeen;

    public Long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(Long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public Long getPeerID() {
        return peerID;
    }

    public void setPeerID(Long peerID) {
        this.peerID = peerID;
    }

    public Deque<Integer> getPrefs() {
        return prefs;
    }

    public void setPrefs(Deque<Integer> prefs) {
        this.prefs = prefs;
    }
}
