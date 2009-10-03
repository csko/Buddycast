package buddycast;

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

    /**
     * TODO: change HashMap to Hashtable
     */
    private String prefix;
    private final int maxConnRandPeers = 10;
    private final double alpha = 0.5;

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
    HashMap connR; // Peer ID, last seen
    /* Unconnectible taste buddies */
    List<Node> unconnT;
    /* Connection Candidates */
    HashMap candidates; // Peer ID, similarity
    List<Node> recv_block_list; // id, timestamp
    List<Node> send_block_list; // id, timestamp
    boolean connectible;

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
     * Select a taste buddy or a random peer from the connection candidate list.
     * @param alpha from [0, 1) is the weight factor between randomness and taste (smaller alpha -> more randomness).
     */
    private int tasteSelectTarget(double alpha) {
        int targetName = -1;
        if (candidates.isEmpty()) {
            return targetName; // no target
        }
        int r = CommonState.r.nextInt(10);  // [0.9] random number

        if (r < (int) alpha * 10) {  /* Select a taste buddy */
            targetName = selectTasteBuddy();
        } else { /* Select a random peer */
            targetName = selectRandomPeer();
        }
        return targetName;

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

    private BuddyCastMessage createBuddyCastMessage(int targetName) {
        BuddyCastMessage msg = new BuddyCastMessage();
        //msg.myPrefs = getMyPreferences(num_myprefs);
        //msg.randomPeers = getRandomPeers(num_rps, targetName);
        msg.connectible = connectible;
        return msg;
    }

    /**
     * Returns random peers not including the targetName peer.
     * @param num The number of random peers returned (at most).
     * @param targetName Not including this peer.
     * @return The random peers.
     */
    private HashMap getRandomPeers(int num, int targetName) {
        HashMap randomPeers = new HashMap(); // peer ID, long timestamp
        Long now = new Date().getTime();

        /* We don't want more peers than available, let's pick some of them */
        if (num <= maxConnRandPeers) {
            ArrayList ids = new ArrayList(connR.keySet());
            ids.remove((Integer) targetName); /* Not including targetName */
            Collections.shuffle(ids, CommonState.r);
            Iterator i = ids.iterator();
            for (int n = 0; n < num && i.hasNext(); n++) {
                Integer id = (Integer) i.next();
                randomPeers.put(id, now);
            }
        } else { /* We want more peers than available, let's return them all */
            Iterator i = connR.keySet().iterator();
            while (i.hasNext()) { /* Copy all the peers */
                if ((Integer) i.next() != targetName) { /* Not including targetName */
                    randomPeers.put(i.next(), now); /* Update last seen time */
                }
            }
        }
        return randomPeers;
    }

    private int getSimilarity(int peerName) {
        int sim = 0;
        //ArrayList prefsCopy = getMyPreferences(0);

        return sim;
    }
}
