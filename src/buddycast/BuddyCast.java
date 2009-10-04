package buddycast;

import java.util.*;
import java.util.ArrayList;
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
    private final int numMyPrefs = 50;
    /**
     * The exploration-to-exploitation ratio.
     */
    private final double alpha = 0.5;
    private final long block_interval = 10000; /* TODO */

    /**
     * The number of random peers in a message.
     */
    private final int num_rps = 10;

    /**
     *
     * @param prefix
     */
    public BuddyCast(String prefix) {
        this.prefix = prefix;
        /* Initialization of the collections*/
        connT = new ArrayList<TasteBuddy>();
        connR = new Hashtable<Integer, Long>();
        unconnT = new ArrayList<Node>();
        candidates = new Hashtable<Integer, Integer>();
        recv_block_list = new Hashtable<Integer, Long>();
        send_block_list = new Hashtable<Integer, Long>();
        /* Always connectible */
        connectible = true;
        myPreferences = new ArrayDeque<Integer>();
        peerPreferences = new Hashtable<Integer, Deque<Integer>>();
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
    Hashtable<Integer, Long> connR; // Peer ID, last seen
    /* Unconnectible taste buddies */
    List<Node> unconnT;
    /* Connection Candidates */
    Hashtable<Integer, Integer> candidates; // Peer ID, similarity
    /* TODO: priority queue and set? */
    Hashtable<Integer, Long> recv_block_list; // Peer ID, timestamp
    Hashtable<Integer, Long> send_block_list; // Peer ID, timestamp
    boolean connectible;
    /**
     * My preferences.
     */
    Deque<Integer> myPreferences;
    Hashtable<Integer, Deque<Integer>> peerPreferences;

    public void work() {
        /**
         * TODO: wait(DT time units) {15 seconds in current implementation}
         */
        /**
         * Remove any peer from the receive and send block lists
         * if its time was expired.
         */
        updateBlockLists();

        /**
         * TODO: If C_C is empty, do bootstrapping
         * select 5 peers from megacache
         */
        int peer = tasteSelectTarget(alpha);
        /**
         * TODO: connectPeer(peer);
         */
        blockPeer(peer, send_block_list);

    /**
     * TODO: remove Q from C_C
     */
    }

    /**
     * Protocol related functions
     */
    public ArrayList selectRecentPeers(int number) {
        return new ArrayList(); /* TODO */
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
        blockPeer(targetName, recv_block_list);
    }

    private void removeCandidate(int targetName) {
        candidates.remove(targetName);
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
        double r = CommonState.r.nextDouble();  // [0, 1) uniformly

        if (r < alpha) {  /* Select a taste buddy */
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
        msg.myPrefs = getMyPreferences(numMyPrefs);
        msg.randomPeers = getRandomPeers(num_rps, targetName);
        msg.connectible = connectible;
        return msg;
    }

    /**
     * Returns random peers not including the targetName peer.
     * @param num The number of random peers returned (at most).
     * @param targetName Not including this peer.
     * @return The random peers.
     */
    private Hashtable<Integer, Long> getRandomPeers(int num, int targetName) {
        Hashtable<Integer, Long> randomPeers = new Hashtable<Integer, Long>(); // peer ID, long timestamp
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
                Integer id = (Integer) i.next();
                if (id != targetName) { /* Not including targetName */
                    randomPeers.put(id, now); /* Update last seen time */
                }
            }
        }
        return randomPeers;
    }

    private int getSimilarity(int peerName) {
        int sim = 0;
        Iterator it = myPreferences.iterator();
        Deque<Integer> peerPrefList = getPrefList(peerName);
        while (it.hasNext()) {
            Integer pref = (Integer) it.next();
            if (peerPrefList.contains(pref)) {
                sim++;
            }
        }
        if (sim == 0) {
            return 0;
        }
        return (int) (1000 * (double) (sim) / Math.sqrt(myPreferences.size() * peerPrefList.size()));
    }

    private Deque<Integer> getMyPreferences(int num) {
        /* TODO: update my preferences */
        if (num == 0) {
            return myPreferences;
        } else {
            ArrayDeque<Integer> result = new ArrayDeque<Integer>();
            Iterator it = myPreferences.descendingIterator();
            for (int i = 0; i < num && it.hasNext(); i++) {
                result.add((Integer) it.next());
            }
            return result;
        }
    }

    Deque<Integer> getPrefList(int peerName) {
        return peerPreferences.get(peerName);
    }

    private boolean isBlocked(int peerName, Hashtable<Integer, Long> list) {
        /* peerName is not on block_list */
        if (!list.containsKey(peerName)) {
            return false;
        }

        if (new Date().getTime() >= list.get(peerName)) {
            list.remove(peerName);
            return false;
        }
        return true;
    }

    private void blockPeer(int peerName, Hashtable<Integer, Long> list) {
        list.put(peerName, new Date().getTime() + block_interval);
    }

    private void updateBlockLists() {
        Long now = new Date().getTime();

        Iterator i = send_block_list.entrySet().iterator();
        while (i.hasNext()) {
            Long timestamp = (Long) i.next();
            if (now >= timestamp) {
                i.remove();
            }
        }

        i = recv_block_list.entrySet().iterator();
        while (i.hasNext()) {
            Long timestamp = (Long) i.next();
            if (now >= timestamp) {
                i.remove();
            }
        }

    }
}
