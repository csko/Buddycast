package buddycast;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import peersim.config.FastConfig;
import peersim.core.*;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

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
        System.err.println("EVENT: " + node + " - " + pid + " " + event);

        Linkable linkable = (Linkable) node.getProtocol(FastConfig.getLinkable(pid));

        if (linkable.degree() > 0) {
            Node peern = linkable.getNeighbor(CommonState.r.nextInt(linkable.degree()));

            if (!peern.isUp()) {
                return;
            }

            BuddyCast peer = (BuddyCast) peern.getProtocol(pid);

            ((Transport) node.getProtocol(FastConfig.getTransport(pid))).send(
                    node,
                    peern,
                    new BuddyCast(prefix),
                    pid);
        }
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

    /* Connectible taste buddies */
    List<TasteBuddy> connT;
    /* Connectible random buddies */
    List<Node> connR;
    /* Unconnectible taste buddies */
    List<Node> unconnT;
    /* Connection Candidates */
    HashMap candidates; // Peer ID, similarity
    List<Node> recv_block_list; // id, timestamp
    List<Node> send_block_list; // id, timestamp

    /**
     * Protocol related functions
     */
    public ArrayList selectRecentPeers(int number) {
        return new ArrayList();
    }

    public void sendBuddyCastMessage(int targetName) {
        /**
         *  TODO: connect to peer
         */
        /**
         * Remove from connection candidates if it was in that list.
         */
        removeCandidate(targetName);

        /**
         * We wont' be sending messages to this peer for a while.
         */
        blockPeer(targetName);
    }

    private void removeCandidate(int targetName) {
        candidates.remove(targetName);
    }

    private void blockPeer(int targetName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Selects the most similar taste buddy from the connection candidates list.
     * @return The ID of the peer.
     */
    private int selectTasteBuddy() {
        int maxId = -1; /* The id of the buddy */
        int maxSimilarity = -1; /* The similarity of the buddy */
        Iterator ids = candidates.keySet().iterator();
        while (ids.hasNext()) {
            Integer id = (Integer) ids.next();
            Integer similarity = (Integer) candidates.get(id);
            if (similarity > maxSimilarity) {
                maxId = id;
                maxSimilarity = similarity;
            }
        }
        return maxId;
    }

    /**
     * Selects a random peer from the connection candidates list.
     * @return The ID of the peer.
     */
    private int selectRandomPeer() {
        int i = 0;
        int r = CommonState.r.nextInt(candidates.size());
        Iterator ids = candidates.keySet().iterator();
        while (ids.hasNext()) {
            Integer id = (Integer) ids.next();
            if (i == r) {
                return id;
            }
            i++;
        }
        return -1;
    }
}
