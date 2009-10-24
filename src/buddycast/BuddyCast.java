package buddycast;

import java.util.*;
import java.util.ArrayList;
import peersim.config.FastConfig;
import peersim.core.*;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

/**
 * Event driven implementation of the BuddyCast protocol.
 */
public class BuddyCast
        implements EDProtocol, Linkable {

    private String prefix;
    /**
     * Constants. TODO: make them changeable via parameters.
     */
    private final int numMyPrefs = 50;
    /**
     * The exploration-to-exploitation ratio.
     */
    private final double alpha = 0.5;
    private final long blockInterval = 4 * 60 * 60 * 1000; // millisecundums
    /**
     * The number of random peers in a message.
     */
    private final int num_rps = 10;
    /**
     * Simulation related.
     */
    private final long timeout = 5 * 60 * 1000; // millisecundums
    /**
     * TODO: It would be nice to somehow reduce the number of lists here.
     * TODO: change iterators to the ":" syntax
     */
    /**
     * Peer containers.
     */
    private static Hashtable<Long, Node> idToNode = new Hashtable<Long, Node>();
    private static Hashtable<Long, Integer> idToSimilarity = new Hashtable<Long, Integer>();
    private static Hashtable<Long, Long> idToConnTime = new Hashtable<Long, Long>();
    /* List of active TCP connections */
    Hashtable<Long, Long> connections; // Peer ID, last seen
    /**
     * These three containers make up the Connection List C_C.
     */
    /* Connectible taste buddies */
    Hashtable<Long, Long> connT; // Peer ID, last seen
    /**
     * The maximum number of connectible taste buddies stored.
     * TODO: this should be changeable
     */
    private int maxConnT = 10;
    /* Connectible random peers */
    Hashtable<Long, Long> connR; // Peer ID, last seen
    /**
     * The maximum number of connectible random peers stored.
     * TODO: this should be changeable
     */
    private int maxConnR = 10;
    /* Unconnectible taste buddies */
    Hashtable<Long, Long> unconnT; // Peer ID, last seen
    /**
     * The maximum number of unconnectible peers stored.
     * TODO: this should be changeable
     */
    private int maxUnConnT = 10;
    /* Connection Candidates */
    Hashtable<Long, Integer> candidates; // Peer ID, similarity
    /* TODO: priority queue and set? */
    /**
     * Block lists.
     */
    Hashtable<Long, Long> recv_block_list; // Peer ID, timestamp
    Hashtable<Long, Long> send_block_list; // Peer ID, timestamp
    /**
     * Peer connection list.
     * TODO: This could be integrated to an other list.
     */
    //Hashtable<Long, Pair<Long, int>> peers;
    /**
     * Are we connectible?
     */
    boolean connectible;
    /**
     * My preferences.
     */
    Deque<Integer> myPreferences;
    /**
     * Peer preferences.
     */
    Hashtable<Long, Deque<Integer>> peerPreferences;

    public BuddyCast(String prefix) {
        this.prefix = prefix;
        /* Initialization of the collections */
        connections = new Hashtable<Long, Long>();
        connT = new Hashtable<Long, Long>(maxConnT);
        connR = new Hashtable<Long, Long>(maxConnR);
        unconnT = new Hashtable<Long, Long>(maxUnConnT);
        candidates = new Hashtable<Long, Integer>();
        recv_block_list = new Hashtable<Long, Long>();
        send_block_list = new Hashtable<Long, Long>();
        /* Always connectible */
        connectible = true;
        myPreferences = new ArrayDeque<Integer>();
        peerPreferences = new Hashtable<Long, Deque<Integer>>();
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
        } // This never happens.
        return bc;
    }

    public void work() {
        System.out.println("work()" + CommonState.getNode());
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
        
        /**
         * Select the Q peer.
         */
        long peer = tasteSelectTarget(alpha);
        int response = connectPeer(peer);
        /**
         * Remove from connection candidates if it was in that list.
         */
        removeCandidate(peer);

        /**
         * We wont' be sending messages to this peer for a while.
         */
        blockPeer(peer, send_block_list);
        if (response == 0) { /* If connected successfully */
            BuddyCastMessage msg = createBuddyCastMessage(peer);
            /**
             *  TODO: send message
             */
        }

    }

    /**
     * 
     * @param superpeers
     */
    private void bootstrap(List<Long> superpeers) {
        Long now = new Date().getTime();
        for (Long peerID : superpeers) {
            addPeer(peerID);
            updateLastSeen(peerID, now);
        }
    }

    /**
     * Protocol related functions
     */
    /**
     * Selects the last few number of peers according to the last seen time.
     * @param list The list to select peers from.
     * @param number The number of peers to select.
     * @return The peer list.
     */
    public ArrayList<Long> selectRecentPeers(Hashtable<Long, Long> list, int number) {
        ArrayList<Long> ret = new ArrayList<Long>(); // the resulting peer IDs
        ArrayList<IDTimePair> peers = new ArrayList<IDTimePair>();
        for (Long peerID : list.keySet()) {
            /* Fill the peers array with the peerID/LastSeen pairs */
            peers.add(new IDTimePair(peerID, list.get(peerID)));
        }
        Collections.sort(peers);
        int i = 0;
        for (IDTimePair pair : peers) {
            if (i++ < number) {
                ret.add(pair.first); // Add the peer ID
            } else {
                break;
            }
        }
        return ret;
    }

    private int connectPeer(long targetName) {
        return 0; /* TODO */
    }

    private void removeCandidate(long targetName) {
        candidates.remove(targetName);
    }

    /**
     * Select a taste buddy or a random peer from the connection candidate list.
     * @param alpha from [0, 1) is the weight factor between randomness and taste (smaller alpha -> more randomness).
     */
    private long tasteSelectTarget(double alpha) {
        System.out.println("tasteSelectTarget()");
        long targetName = -1;
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
    private long selectTasteBuddy() {
        long maxId = -1; /* The id of the buddy */
        int maxSimilarity = -1; /* The similarity of the buddy */
        Iterator ids = candidates.keySet().iterator();
        while (ids.hasNext()) {
            Long id = (Long) ids.next();
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

    private BuddyCastMessage createBuddyCastMessage(long targetName) {
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
    private Hashtable<Long, Long> getRandomPeers(int num, long targetName) {
        Hashtable<Long, Long> randomPeers = new Hashtable<Long, Long>(); // peer ID, long timestamp
        Long now = new Date().getTime();

        /* We don't want more peers than available, let's pick some of them */
        if (num <= maxConnR) { // TODO: is this correct?
            ArrayList ids = new ArrayList(connR.keySet());
            ids.remove(targetName); /* Not including targetName */
            Collections.shuffle(ids, CommonState.r);
            Iterator i = ids.iterator();
            for (int n = 0; n < num && i.hasNext(); n++) {
                Long id = (Long) i.next();
                randomPeers.put(id, now);
            }
        } else { /* We want more peers than available, let's return them all */
            Iterator i = connR.keySet().iterator();
            while (i.hasNext()) { /* Copy all the peers */
                Long id = (Long) i.next();
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

    Deque<Integer> getPrefList(long peerName) {
        return peerPreferences.get(peerName);
    }

    private boolean isBlocked(long peerName, Hashtable<Long, Long> list) {
        /* peerName is not on block_list */
        if (!list.containsKey(peerName)) {
            return false;
        }

        /* Remove it if it's expired */
        if (new Date().getTime() >= list.get(peerName)) {
            list.remove(peerName);
            return false;
        }
        return true;
    }

    private void blockPeer(long peerName, Hashtable<Long, Long> list) {
        list.put(peerName, new Date().getTime() + blockInterval);
    }

    private void updateBlockLists() {
        Long now = new Date().getTime();

        /* Remove outdated entries */
        Iterator i = send_block_list.values().iterator();
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

    private void addPeerToConnList(long peerID, boolean connectible) {
        // TODO: assert( isConnected(peer_name) );

        // See if the peer is already on one of the lists and remove

        connT.remove(peerID);
        connR.remove(peerID);
        unconnT.remove(peerID);

        Long now = new Date().getTime();
        if (connectible) {
            if (!addPeerToConnT(peerID, now)) {
                addPeerToConnR(peerID, now);
            }
        } else {
            addPeerToUnConnT(peerID, now);
        }

    }

    public int degree() {
        return connections.size();
    }

    public Node getNeighbor(int i) {
        if (i < 0 || i >= degree()) {
            throw new IndexOutOfBoundsException();
        }
        Collection<Long> values = connections.values();
        Object[] valuesArray = values.toArray();

        assert (valuesArray.length == degree());

        return idToNode.get((Long) valuesArray[i]);

    }

    public boolean addNeighbor(Node node) {
        if (contains(node)) {
            return false;
        }
        /* TODO */
        Long peerName = node.getID();
        //int dummy = addPeer(peerName); // TODO
        Long now = new Date().getTime();
        updateLastSeen(peerName, now);
        connections.put(peerName, now + timeout);
        return true;
    }

    private void removeNeighbor(Long peerID) {
        if (connections.containsKey(peerID)) {
            //updateLastSeen(peerID, new Date().getTime()); // TODO
            connections.remove(peerID);
            //initiate_connections.remove(peerID); // TODO

            connT.remove(peerID);
            connR.remove(peerID);
            unconnT.remove(peerID);
        }
    }

    public boolean contains(Node node) {
        if (connections.containsKey(node.getID())) {
            return true;
        } else {
            return false;
        }
    }

    public void pack() {
    }

    public void onKill() {
        connections = null;
        connT = null;
        connR = null;
        unconnT = null;
        candidates = null;
        recv_block_list = null;
        send_block_list = null;
        connectible = false;
        myPreferences = null;
        peerPreferences = null;
    }

    private void addPeerToConnR(long peerID, Long now) {
        if (!connR.contains(peerID)) {
            long outPeer = addNewPeerToConnList(connR, maxConnR, peerID, now);
            if (outPeer != -1) {
                closeConnection(outPeer);
            }
        }
    }

    private void addPeerToUnConnT(long peerID, Long now) {
        if (!unconnT.contains(peerID)) {
            long outPeer = addNewPeerToConnList(unconnT, maxUnConnT, peerID, now);
            if (outPeer != -1) {
                closeConnection(outPeer);
            }
        }
    }

    /**
     * Add a peer to the taste buddy list.
     * @param peerID The peer ID to add.
     * @param now The peer's connection time.
     * @return True, if the peer was added, false otherwise.
     */
    boolean addPeerToConnT(long peerID, Long now) {
        int sim = 2;
        // sim = peers[peerID].second; // TODO: get the similarity

        if (sim > 0) {
            /* The list is not full, we don't have to remove */
            if (connT.size() <= maxConnT) {
                connT.put(peerID, now);
                return true;
            } else {
                /* Get the peer with minimal similarty */
                Long minPeerTime = now + 1;
                long minPeerID = -1;
                int minSim = Integer.MAX_VALUE;

                for (Iterator i = connT.keySet().iterator(); i.hasNext();) {
                    Long peer = (Long) i.next();
                    Long peerTime = connT.get(peer);
                    int peerSim = 1; // TODO: get the similarity

                    if (peerSim < minSim ||
                            (peerSim == minSim && peerTime < minPeerTime)) {
                        minPeerID = peer;
                        minPeerTime = peerTime;
                        minSim = peerSim;
                    }
                }
                /* There is a less similar peer to drop */
                if (sim > minSim) {
                    /* Remove the least similar peer */
                    connT.remove(minPeerID);
                    /* Try to add the peer to the random peer list */
                    addPeerToConnR(minPeerID, minPeerTime);
                    /* Add the new peer to the buddy list */
                    connT.put(peerID, now);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A general method to add a candidate peer to a connection list.
     * @param connList The connection list to manipulate.
     * @param maxNum The maximum number of peers that list can hold.
     * @param peerID The peer ID to add.
     * @param connTime The peer's connection time.
     * @return The ID of the peer that was removed from the list; -1 if none
     * were removed.
     */
    private long addNewPeerToConnList(Hashtable<Long, Long> connList,
            int maxNum, long peerID, Long connTime) {
        long oldestPeerID = -1;
        long oldestPeerTime = connTime + 1;

        /* The list is not full, we don't have to remove */
        if (connList.size() <= maxNum) {
            connList.put(peerID, connTime);
            return oldestPeerID; /* none removed */
        } else {
            /* Get the oldest peer */
            for (Iterator i = connList.keySet().iterator(); i.hasNext();) {
                Long peer = (Long) i.next();
                Long peerTime = connList.get(peer);
                /* NOTE: we might want to select between the oldest peers (if
                 * there are more of them) based on some probability */
                if (peerTime < oldestPeerTime) { /* Found a new oldest peer */
                    oldestPeerID = peer;
                    oldestPeerTime = peerTime;
                }
            }

            /* The new peer is newer than the oldest peer */
            if (connTime > oldestPeerTime) {
                /* Remove the old peer */
                connList.remove(oldestPeerID);
                /* Add the new peer */
                connList.put(peerID, connTime);
                return oldestPeerID;
            }
            return peerID;
        }
    }

    int addPeer(long peerID) {
        /* TODO: check if the peer is myself */
        //if (peerID == name) {
//            return -1;
//        }

        /* TODO: this might need some refactoring */
        if (!idToConnTime.containsKey(peerID) &&
                !idToSimilarity.containsKey(peerID)) {
            idToConnTime.put(peerID, new Long(0));
            idToSimilarity.put(peerID, new Integer(0));
            return 1;
        }
        return 0;
    }

    private void closeConnection(long peerID) {
        if (connections.containsKey(peerID)) {
            updateLastSeen(peerID, new Date().getTime());
            connections.remove(peerID);
            //initiate_connections.remove(peerID);
            connT.remove(peerID);
            connR.remove(peerID);
            unconnT.remove(peerID);
        }
    }

    private void updateLastSeen(long peerID, Long lastSeen) {
        if (idToConnTime.containsKey(peerID)) {
            idToConnTime.put(peerID, lastSeen);
        }
    }

    /**
     * A pair representing a peer ID and a last seen time.
     */
    private final class IDTimePair implements Comparable {

        private final Long first, second;

        IDTimePair(Long first, Long second) {
            this.first = first;
            this.second = second;
        }

        public int compareTo(Object p) {
            /* Oldest peers first */
            IDTimePair pair = (IDTimePair) p;
            if (this.second < pair.second) {
                return 1;
            } else if (this.second == pair.second) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    public static void main(String args[]) {
        Hashtable<Long, Long> connections = new Hashtable<Long, Long>();
        connections.put(new Long(0), new Long(1));
        connections.put(new Long(1), new Long(2));
        connections.put(new Long(2), new Long(3));
        connections.put(new Long(3), new Long(4));


        Collection<Long> values = connections.values();
        Object[] valuesArray = values.toArray();
        for(int i=0; i<valuesArray.length; i++){
            System.out.println(valuesArray[i]);
        }

        System.out.println("---");

        connections.put(new Long(4), new Long(5));
        connections.put(new Long(3), new Long(24));
        values = connections.values();
        valuesArray = values.toArray();
        for(int i=0; i<valuesArray.length; i++){
            System.out.println(valuesArray[i]);
        }

    }
}
