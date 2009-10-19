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
        implements EDProtocol, Linkable {

    private String prefix;
    /**
     * Constants. TODO: make them changeable via parameters.
     */
    private final int maxConnRandPeers = 10;
    private final int numMyPrefs = 50;
    /**
     * The exploration-to-exploitation ratio.
     */
    private final double alpha = 0.5;
    private final long block_interval = 4 * 60 * 60 * 1000; // millisecundums

    /**
     * The number of random peers in a message.
     */
    private final int num_rps = 10;
    /**
     * Simulation related.
     */
    private final long timeout = 5 * 60 * 1000; // millisecundums
    /**
     * Peer containers.
     */
    private static Hashtable<Long, Node> idToNode = new Hashtable<Long, Node>();
    /* List of active TCP connections */
    Hashtable<Long, Long> connections; // Peer ID, timestamp
    /**
     * These three containers make up the Connection List C_C.
     */
    /* Connectible taste buddies */
    Hashtable<Long, TasteBuddy> connT; // Peer ID, TasteBuddy
    /* Connectible random peers */
    Hashtable<Long, Long> connR; // Peer ID, last seen
    /**
     * The maximum number of connectible random peers stored.
     * TODO: this should be changeable
     */
    private int maxConnR = 10;
    /* Unconnectible taste buddies */
    List<Long> unconnT; // Peer ID
    /* Connection Candidates */
    Hashtable<Long, Integer> candidates; // Peer ID, similarity
    /* TODO: priority queue and set? */
    /**
     * Block lists.
     */
    Hashtable<Long, Long> recv_block_list; // Peer ID, timestamp
    Hashtable<Long, Long> send_block_list; // Peer ID, timestamp
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

    /**
     *
     * @param prefix
     */
    public BuddyCast(String prefix) {
        this.prefix = prefix;
        /* Initialization of the collections */
        connections = new Hashtable<Long, Long>();
        connT = new Hashtable<Long, TasteBuddy>();
        connR = new Hashtable<Long, Long>();
        unconnT = new ArrayList<Long>();
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
     * Protocol related functions
     */
    public ArrayList selectRecentPeers(int number) {
        return new ArrayList(); /* TODO */
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
        if (num <= maxConnRandPeers) {
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

        if (new Date().getTime() >= list.get(peerName)) {
            list.remove(peerName);
            return false;
        }
        return true;
    }

    private void blockPeer(long peerName, Hashtable<Long, Long> list) {
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

    private void addPeerToConnList(long peerID, boolean connectible) {
        // TODO: assert( isConnected(peer_name) );

        // See if the peer is already on one of the lists and remove
        if (connT.containsKey(peerID)) {
            connT.remove(peerID);
        } else if (connR.containsKey(peerID)) {
            connR.remove(peerID);
        } else if (unconnT.contains(peerID)) {
            unconnT.remove(peerID);
        }

        Long now = new Date().getTime();
        if (connectible) {
            if (!addPeerToConnT(peerID, now)) {
                addPeerToConnR(peerID, now);
            }
        } else {
            addPeerToUnConn(peerID, now);
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
        Long[] valuesArray = (Long[]) values.toArray();

        assert (valuesArray.length == degree());

        return idToNode.get(valuesArray[i]);

    }

    public boolean addNeighbor(Node node) {
        if (contains(node)) {
            return false;
        }
        /* TODO */
        Long peerName = node.getID();
        //int dummy = addPeer(peerName); // TODO
        Long now = new Date().getTime();
        //updateLastSeen(peerName, now); // TODO
        connections.put(peerName, now + timeout);
        return true;
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

    private boolean addPeerToConnT(long peerID, Long now) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void addPeerToConnR(long peerID, Long now) {
        if (!unconnT.contains(peerID)) {
            int outPeer = addNewPeerToConnList(connR, maxConnR, peerID, now);
            if (outPeer != -1) {
//                closeConnection(outPeer);
            }

        }
        /*
        if (connected_unconnectable_peers.find(peer_name) == connected_unconnectable_peers.end()) {
        int out_peer = addNewPeerToConnList(connected_unconnectable_peers, max_conn_up, peer_name, conn_time);
        if (out_peer != -1 * (NUM_SUPERPEERS + 1)) {
        closeConnection(out_peer);
        }
        }
         */

    }

    private void addPeerToUnConn(long peerID, Long now) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * A general method to add a candidate peer to a connection list.
     */
    private int addNewPeerToConnList(Hashtable<Long, Long> connList, int maxNum, long peerID, Long connTime) {
        /*        int dummy = -1; // TODO
        pair<int32_t, string> oldest_peer, to_cmp;
        String _name;
        Long _conn_time, r;
        vector<string> out_peer;
        stringstream pfirst;

        // cerr << name << "'s addNewPeerToConnList() called with peer_name = " << peer_name << endl;
        if (connList.size() <= maxNum) {
        connList[peerID] = connTime;
        return dummy; // return empty string
        } else {
        oldest_peer = pair < int32_t
        , string>(connTime+1,

        "");
        string initial("abcdefghijklmnopqrstuvwxyz");
        for (map<int, int32_t>:  {
        :
        }
        iterator p = connList.begin();
        p!=connList.end();
        p++


        )
        {
        _conn_time = p-  > second;
        r = randint(0, max_conn_tb + 1);
        _name.clear();
        _name.push_back(initial.at(r));
        _name.push_back('.');
        pfirst << p->first;
        _name += pfirst.str();
        pfirst.str("");  // clear
        to_cmp = pair < int32_t
        , string>(_conn_time,
        _name)
        ;


        oldest_peer = min_peer

        (oldest_peer

        , to_cmp);
        }
        if (connTime > oldest_peer.first)
        {
        out_peer = split_dot(oldest_peer.second);
        connList.erase(atoi(out_peer[1].c_str()));
        connList[peerID] = connTime;
        return atoi(out_peer[1].c_str());
        }
        return peerID;
        }
         */
        return 0;
    }
}
