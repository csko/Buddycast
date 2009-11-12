package buddycast;

import java.util.*;
import peersim.config.FastConfig;
import peersim.core.*;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

/**
 * Event driven implementation of the BuddyCast protocol.
 */
public class BuddyCast
        implements EDProtocol, Linkable {

    private String prefix;
// ======================== network related ========================
// =================================================================
    /**
     * The number of Super Peers. TODO: this should be changeable.
     * NOTE: The first s peers are considered Super Peers (0, ..., s-1).
     */
    //final static int numSuperPeers = 5;
    final static int numSuperPeers = 0; // Don't use superpeers
    /**
     * The array of the superpeers.
     */
    private static ArrayList<Long> superpeers = new ArrayList<Long>();

    /**
     * Create superpeers.
     */
    static {
        for (int i = 0; i < numSuperPeers; i++) {
            superpeers.add(new Long(i));
        }
    }
    /**
     * This is true when we are in the initialization state.
     * It is useful to use this when we have numSuperPeers=0, because
     * when we set up an overlay network through an other initializer, we
     * might want to fill the candidates list too. If this init variable is
     * true, the addNeighbor method will also add the peer to the candidates
     * list.
     */
    boolean useInit = false;
// ======================== peer containers ========================
// =================================================================
    /**
     * TODO: It would be nice to somehow reduce the number of lists here.
     */
    private static Hashtable<Long, Node> idToNode;
    private Hashtable<Long, Integer> idToSimilarity;
    private Hashtable<Long, Long> idToConnTime;
    /* List of active TCP connections */
    Hashtable<Long, Long> connections; // Peer ID, keep-alivetimeout
    /**
     * These three containers make up the Connection List C_C.
     */
    /* Connectible taste buddies */
    Hashtable<Long, Long> connT; // Peer ID, last seen
    /* Connectible random peers */
    Hashtable<Long, Long> connR; // Peer ID, last seen
    /* Unconnectible taste buddies */
    Hashtable<Long, Long> unconnT; // Peer ID, last seen
    /* Connection Candidates */
    Hashtable<Long, Long> candidates; // Peer ID, timestamp
    /**
     * Block lists.
     */
    Hashtable<Long, Long> recvBlockList; // Peer ID, timestamp
    Hashtable<Long, Long> sendBlockList; // Peer ID, timestamp
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
// ======================== peer container limits ==================
// =================================================================
    /* TODO: these should be changeable */
    /**
     * The maximum number of connectible taste buddies stored.
     */
    private int maxConnT = 10;
    /**
     * The maximum number of unconnectible peers stored.
     */
    private int maxUnConnT = 10;
    /**
     * The maximum number of connectible random peers stored.
     */
    private int maxConnR = 10;
    /**
     * The maximum number of connection candidates stored.
     */
    private int maxCandidates = 50;
    /**
     * The number of maximum preferences stored per peer.
     */
    private final int maxPeerPreferences = 500;
// ======================== message and behavior ===================
// =================================================================
    /**
     * Time to wait before doing the buddycast protocol.
     */
    static final long timeToWait = 15;
    /**
     * The exploitation-to-exploration ratio.
     * (the higher it is, exploration is done with a higher probability)
     */
    private final double alpha = 0.5;
    private final long blockInterval = 4 * 60 * 60;
    /**
     * The number of my own preferences sent in a message.
     */
    private final int numMsgMyPrefs = 50;
    /**
     * The number of random peers in a message.
     */
    private final int numMsgRandomPeers = 10;
    /**
     * The number of Taste Buddies in a message.
     */
    private final int numMsgTasteBuddies = 10;
    /**
     * The number of Taste Buddy preferences in a message.
     */
    private final int numMsgTBPrefs = 20;
    /**
     * The connection timeout.
     */
    private final long timeout = 5 * 60;
// ======================== initialization =========================
// =================================================================

    private int protocolID; // TODO: static

    /**
     * Construtor. Creates the lists.
     * @param prefix The prefix of the protocol.
     */
    public BuddyCast(String prefix) {
        this.prefix = prefix;
        /* Initialization of the collections */
        createLists();
        /* Always connectible */
        connectible = true;
        protocolID = CommonState.getPid();
    }

    /**
     * Create the lists.
     */
    private void createLists() {
        connections = new Hashtable<Long, Long>();
        connT = new Hashtable<Long, Long>(maxConnT);
        connR = new Hashtable<Long, Long>(maxConnR);
        unconnT = new Hashtable<Long, Long>(maxUnConnT);
        candidates = new Hashtable<Long, Long>(maxCandidates);
        recvBlockList = new Hashtable<Long, Long>();
        sendBlockList = new Hashtable<Long, Long>();
        myPreferences = new ArrayDeque<Integer>(numMsgMyPrefs);
        peerPreferences = new Hashtable<Long, Deque<Integer>>();
        idToNode = new Hashtable<Long, Node>();
        idToSimilarity = new Hashtable<Long, Integer>();
        idToConnTime = new Hashtable<Long, Long>();
    }

// ======================== Linkable ===============================
// =================================================================
    public int degree() {
        return connections.size();
    }

    public Node getNeighbor(int i) {
        if (i < 0 || i >= degree()) {
            throw new IndexOutOfBoundsException();
        }
        Collection<Long> values = connections.keySet();
        Object[] valuesArray = values.toArray();

        assert (valuesArray.length == degree());

        return idToNode.get((Long) valuesArray[i]);

    }

    public boolean addNeighbor(Node node) {
        /* Also known as addConnection() */
        if (contains(node)) {
            return false;
        }
        /* If we don't have the node cached, cache it */
        if (!idToNode.containsKey(node.getID())) {
            idToNode.put(node.getID(), node);
        }
        Long peerID = node.getID();
        addPeer(peerID);
        Long now = CommonState.getTime();
        updateLastSeen(peerID, now);
        connections.put(peerID, now + timeout);
        if (useInit) {
            /* Get the peer's items as if they were sent in a message */
            addPreferences(peerID, ((BuddyCast) node.getProtocol(protocolID)).getMyPreferences(numMsgMyPrefs));
            addConnCandidate(peerID, now);
        }
        return true;
    }

    /* NOTE: this is not a method of Linkable but fits here logically. */
    /**
     * Remove a neighbor.
     * @param peerID The neighbor peer's ID.
     */
    private void removeNeighbor(Long peerID) {

        if (connections.containsKey(peerID)) {
            updateLastSeen(peerID, CommonState.getTime());
            connections.remove(peerID);

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
        recvBlockList = null;
        sendBlockList = null;
        connectible = false;
        myPreferences = null;
        peerPreferences = null;
    }

// ======================== EDProtocol =============================
// =================================================================
    /**
     * This is the standard method to define to process incoming messages.
     * @param node
     * @param pid
     * @param event
     */
    public void processEvent(Node node, int pid, Object event) {
        //System.err.println("[" + "] EVENT: " + node + " - " + pid + " " + event);
        if (event instanceof CycleMessage) {
            /* Cycle message, schedule an other message in timeToWait time*/
            EDSimulator.add(timeToWait, CycleMessage.getInstance(), node, pid);

            /* Do the BuddyCast protocol */
            work(pid);
        } else if (event instanceof BuddyCastMessage) {
            System.out.println("BuddyCastMessage!");
            /* Handle incoming BuddyCast message */
            BuddyCastMessage msg = (BuddyCastMessage) event;

            /* See if the peer is blocked */
            if (isBlocked(msg.sender, recvBlockList)) {
                return;
            }

            int changed = 0;
            changed += addPreferences(msg.sender, msg.myPrefs);

            /* Use the Taste Buddy list provided in the message */
            for (TasteBuddy tb : msg.tasteBuddies.values()) {
                if (addPeer(tb.getPeerID()) == 1) { /* Peer successfully added */
                    updateLastSeen(tb.getPeerID(), tb.getLastSeen());
                }
                changed += addPreferences(tb.getPeerID(), tb.getPrefs());
                addConnCandidate(tb.getPeerID(), tb.getLastSeen());
            }

            /* Use the Random Peer list provided in the message */
            for (Long peer : msg.randomPeers.keySet()) {
                if (addPeer(peer) == 1) { /* Peer successfully added */
                    updateLastSeen(peer, msg.randomPeers.get(peer));
                }
                addConnCandidate(peer, msg.randomPeers.get(peer));
            }

            /* Put the peer on our lists */
            addPeerToConnList(msg.sender, msg.connectible);

            /* If the message wasn't a reply to a previous message */
            if (msg.reply == false) {
                /* If the sender is not blocked as a recipient */
                if (!isBlocked(msg.sender, sendBlockList)) {
                    /* Create the reply message */
                    BuddyCastMessage replyMsg = createBuddyCastMessage(msg.sender);
                    /* Set the sender field */
                    replyMsg.sender = CommonState.getNode().getID();
                    /* It's a reply */
                    replyMsg.reply = true;
                    /* Send the message */
                    Node senderNode = getNodeByID(msg.sender);
                    ((Transport) node.getProtocol(FastConfig.getTransport(pid))).send(
                            CommonState.getNode(),
                            senderNode,
                            replyMsg,
                            pid);
                    /* No longer a candidate */
                    removeCandidate(msg.sender);
                    /* Block the peer so we won't send too many messages to them */
                    blockPeer(msg.sender, sendBlockList);
                }
            }
            /* Block the peer so we won't receive too many messages from them */
            blockPeer(msg.sender, recvBlockList);
        }
    }

    /**
     * Cloning method. Copies all the lists.
     * @return The clone object.
     */
    @Override
    public Object clone() {
        BuddyCast bc = null;
        try {
            bc = (BuddyCast) super.clone();
        } catch (CloneNotSupportedException e) {
        } // This never happens.
        bc.connectible = connectible;
        bc.createLists(); // Not really cloning, but creating a new empty object
        return bc;
    }

// ======================== Protocol metohds =======================
// =================================================================
    /**
     * The main function. It should be called about every 15 simulation seconds.
     * @param pid The protocol ID to use.
     */
    private void work(int pid) {
        /**
         * Remove any peer from the receive and send block lists
         * if its time was expired.
         */
        updateBlockLists();

        /**
         * Do bootstrapping if needed.
         */
        if (candidates.isEmpty()) {
            bootstrap();
        }

        if (isSuperPeer(CommonState.getNode().getID())) {
            /* Do nothing as a Super Peer */
            return;
        }
        /**
         * Select the Q peer.
         */
        long peer = tasteSelectTarget(alpha);
        if (peer == -1) {
            /* No valid target found */
            return;
        }
        /* Physically connect to the peer */
        int response = connectPeer(peer);
        /**
         * Remove from connection candidates if it was in that list.
         */
        removeCandidate(peer);

        /**
         * We won't be sending messages to this peer for a while.
         */
        blockPeer(peer, sendBlockList);
        if (response == 0) { /* If connected successfully */
            BuddyCastMessage msg = createBuddyCastMessage(peer);
            /* Set the sender field */
            msg.sender = CommonState.getNode().getID();
            /* Not a reply */
            msg.reply = false;
            /* Send it */
            Node node = getNodeByID(peer);
            ((Transport) node.getProtocol(FastConfig.getTransport(pid))).send(
                    CommonState.getNode(),
                    node,
                    msg,
                    pid);
        }

    }

    /**
     * Do the bootstrapping. Add a maximum of number of superpeers as peers.
     */
    private void bootstrap() {
//	if (bootstrapped)
//		return;

        Long now = CommonState.getTime();
        int i = 0;
        for (Long peerID : superpeers) {
            /* Don't add myself (as a superpeer) */
            if (peerID != CommonState.getNode().getID()) {
                if (i++ < numSuperPeers) {
                    addPeer(peerID);
                    updateLastSeen(peerID, now);
                } else {
                    break;
                }
            }
        }
        /* NOTE: at this point, idToConnTime should only contain superpeers */
        /* Get the superpeers who are not on the block list */
        Hashtable<Long, Long> superPeerList = new Hashtable<Long, Long>();
        for (Long peerID : idToConnTime.keySet()) {
            /* See if the peer is blocked */
            if (!isBlocked(peerID, sendBlockList)) {
                superPeerList.put(peerID, idToConnTime.get(peerID));
            }
        }
        /* Add the most recent peers as candidates */
        ArrayList<Long> recentPeers = selectRecentPeers(superPeerList, numSuperPeers);
        for (Long peerID : recentPeers) {
            addConnCandidate(peerID, idToConnTime.get(peerID));
        }
    }

    private BuddyCastMessage createBuddyCastMessage(long targetName) {
        BuddyCastMessage msg = new BuddyCastMessage();
        msg.myPrefs = getMyPreferences(numMsgMyPrefs);
        msg.tasteBuddies = getTasteBuddies(numMsgTasteBuddies, numMsgTBPrefs, targetName);
        msg.randomPeers = getRandomPeers(numMsgRandomPeers, targetName);
        msg.connectible = connectible;
        return msg;
    }

    private int connectPeer(long peerID) {
        /* TODOdaDon't always successfully connect to the peer */
        int result = 0; /* The connection was successful */

        assert (!isBlocked(peerID, sendBlockList));

        Node node = getNodeByID(peerID);
        if (node == null) { /* Node not found */
            return 1;
        }

        if (result == 0) {
            addNeighbor(node);
            /* Switch to the neighbor node and add us as a neighbor
             * NOTE: This is possibly a hack.
             */
            Node myNode = CommonState.getNode();
            CommonState.setNode(node);
            ((BuddyCast) node.getProtocol(protocolID)).addNeighbor(myNode);
            CommonState.setNode(myNode);
        }

        return result;
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

        if (r < alpha) {  /* Select a taste buddy (exploitation) */
            targetName = selectTasteBuddy();
        } else { /* Select a random peer (exploration) */
            targetName = selectRandomPeer();
        }
        return targetName;

    }

    void setInit(boolean init) {
        this.useInit = init;
    }

// ======================== list handling ==========================
// =================================================================
    /**
     * Adds peer to the connection candidates list.
     *
     * The method checks if the connection candidate list is too long, and
     * if so, deletes the oldest peer from the this list.
     *
     * @param peerID The peer to be added.
     * @param lastSeen The peer's last seen value
     */
    private void addConnCandidate(Long peerID, Long lastSeen) {
        /* See if the peer is blocked */
        if (isBlocked(peerID, sendBlockList)) {
            return;
        }

        /* Already in the list, just update the time */
        if (candidates.contains(peerID)) {
            candidates.put(peerID, lastSeen);
            return;
        }

        /* There is space on the list, so just add it */
        if (candidates.size() < maxCandidates) {
            candidates.put(peerID, lastSeen);
            return;
        } else {/* The list is full, remove the oldest entry */
            /* Find the oldest entry */
            long oldestPeer = -1;
            long oldestPeerTime = Long.MAX_VALUE;
            for (Long peer : candidates.keySet()) {
                Long peerTime = candidates.get(peer);
                if (peerTime < oldestPeerTime) {
                    oldestPeer = peer;
                    oldestPeerTime = peerTime;
                }
            }
            /* Remove the oldest entry */
            removeCandidate(oldestPeer);
        }
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
        if (connList.size() < maxNum) {
            connList.put(peerID, connTime);
            return oldestPeerID; /* none removed */
        } else {
            /* Get the oldest peer */
            for (Long peer : connList.keySet()) {
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

    /**
     * Try to add a peer to the peer list, initializing its connection time
     * and similarity to 0.
     * @param peerID
     * @return -1, if the peer is the same peer, hence not added; 0 if the peer
     * is already in one of the lists; 1 if successfully added
     */
    int addPeer(long peerID) {
        /* Check if the peer is myself */
        if (peerID == CommonState.getNode().getID()) {
            return -1;
        }

        /* TODO: this might need some refactoring */
        if (!idToConnTime.containsKey(peerID) &&
                !idToSimilarity.containsKey(peerID)) {
            idToConnTime.put(peerID, new Long(0));
            idToSimilarity.put(peerID, new Integer(0));
            return 1;
        }
        return 0;
    }

    private void addPeerToConnList(long peerID, boolean connectible) {
        /*
         * See if the peer is already on one of the lists and remove
         */
        connT.remove(peerID);
        connR.remove(peerID);
        unconnT.remove(peerID);

        Long now = CommonState.getTime();
        if (connectible) {
            if (!addPeerToConnT(peerID, now)) {
                addPeerToConnR(peerID, now);
            }
        } else {
            addPeerToUnConnT(peerID, now);
        }
    }

    /**
     * Add a peer to the taste buddy list.
     * @param peerID The peer ID to add.
     * @param now The peer's connection time.
     * @return True, if the peer was added, false otherwise.
     */
    boolean addPeerToConnT(long peerID, Long now) {

        if (connT.containsKey(peerID)) { /* Peer is already on the list */
            return true;
        }

        int sim = idToSimilarity.get(peerID);

        if (sim > 0) {
            /* The list is not full, we don't have to remove */
            if (connT.size() <= maxConnT) {
                connT.put(peerID, now);
                return true;
            } else { /* The list is full, we need to remove the least similar peer */
                /* Get the peer with minimal similarity */
                Long minPeerTime = now + 1;
                long minPeerID = -1;
                int minSim = Integer.MAX_VALUE;

                for (Long peer : connT.keySet()) {
                    Long peerTime = connT.get(peer);
                    int peerSim = idToSimilarity.get(peer);

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
                    /* Add the new peer to the taste buddy list */
                    connT.put(peerID, now);
                    return true;
                }
            }
        }
        return false;
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

    private void blockPeer(long peerID, Hashtable<Long, Long> list) {
        if (peerID == -1) {
            return;
        }
        list.put(peerID, CommonState.getTime() + blockInterval);
    }

    private void updateBlockLists() {
        Long now = CommonState.getTime();
        /* Remove outdated entries */
        Iterator i;
        for (i = sendBlockList.values().iterator(); i.hasNext();) {
            Long timestamp = (Long) i.next();
            if (now >= timestamp) {
                i.remove();
            }
        }
        for (i = recvBlockList.values().iterator(); i.hasNext();) {
            Long timestamp = (Long) i.next();
            if (now >= timestamp) {
                i.remove();
            }
        }
    }

    private void updateLastSeen(long peerID, Long lastSeen) {
        if (idToConnTime.containsKey(peerID)) {
            idToConnTime.put(peerID, lastSeen);
        }
    }

    /**
     * Get a node by its ID.
     * @param id The ID of the node.
     * @return The node, if found; null otherwise.
     */
    private Node getNodeByID(Long id) {
        /* First, see if we have the node cached */
        if (idToNode.contains(id)) {
            return idToNode.get(id);
        }
        /* Find the Node in the network
         * NOTE: this is possibly a hack.
         * The desired way would be to store Node references along the way.
         */
        /* Try the natural index first */
        Node node = Network.get((int) ((long) id));
        if (node.getID() == id) {
            return node;
        }
        for (int i = 0; i < Network.size(); i++) {
            node = Network.get(i);
            if (node.getID() == id) {
                /* Add the node to the cache for later use */
                idToNode.put(id, node);
                return node;
            }
        }
        return null;
    }

    /**
     * Determines if a peer is on the block list.
     * @param peerID The peer's ID.
     * @param list The list to examine.
     * @return true, if the peer is on the list; false otherwise
     */
    private boolean isBlocked(long peerID, Hashtable<Long, Long> list) {
        /* peerID is not on block_list */
        if (!list.containsKey(peerID)) {
            return false;
        }

        /* Remove it if it's expired */
        if (CommonState.getTime() >= list.get(peerID)) {
            list.remove(peerID);
            return false;
        }
        return true;
    }

    boolean isSuperPeer(Long peerID) {
        return 0 <= peerID && peerID < numSuperPeers;
    }

    /**
     * Selects the most similar taste buddy from the connection candidates list.
     * @return The ID of the peer.
     */
    private long selectTasteBuddy() {
        long maxId = -1; /* The id of the buddy */
        int maxSimilarity = -1; /* The similarity of the buddy */
        for (Long peer : candidates.keySet()) {
            Integer similarity = idToSimilarity.get(peer);
            if(similarity == null){ // TODO: remove hack
                idToSimilarity.put(peer, new Integer(0));
                similarity = idToSimilarity.get(peer);
            }
            if(similarity == null){
                System.out.println("");
            }
            if (similarity > maxSimilarity) {
                maxId = peer;
                maxSimilarity = similarity;
            }
        }
        return maxId;
    }

    /**
     * Selects a random peer from the connection candidates list.
     * @return The ID of the peer.
     */
    private long selectRandomPeer() {
        long i = 0;
        int r = CommonState.r.nextInt(candidates.size());
        for (Long peer : candidates.keySet()) {
            if (i == r) {
                return peer;
            }
            i++;
        }
        return -1;
    }

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

    private Hashtable<Long, TasteBuddy> getTasteBuddies(
            int numTBs, int numTBPs, Long targetName) {
        Hashtable<Long, TasteBuddy> tbs = new Hashtable<Long, TasteBuddy>();
        Long now = CommonState.getTime();

        if (numTBs < numMsgTasteBuddies) {
            Vector<Long> ctb = new Vector<Long>(); // Connected taste buddies
            for (Long peer : connT.keySet()) {
                ctb.add(peer);
            }
            Deque<Long> tb_list = (Deque<Long>) randomSelectList(ctb, numTBs);
            for (Long peer : tb_list) {
                /* Not including the target */
                if (!peer.equals(targetName)) {
                    /* Set up the taste buddy */
                    TasteBuddy tb = new TasteBuddy();
                    tb.setPrefs(
                            (Deque<Integer>) randomSelectList(
                            peerPreferences.get(peer),
                            numTBPs));
                    tb.setLastSeen(now);
                    tb.setPeerID(peer);

                    tbs.put(peer, tb);
                }
            }
        } else {
            for (Long peer : connT.keySet()) {
                /* Not including the target */
                if (!peer.equals(targetName)) {
                    /* Set up the taste buddy */
                    TasteBuddy tb = new TasteBuddy();
                    tb.setPrefs(
                            (Deque<Integer>) randomSelectList(
                            peerPreferences.get(peer),
                            numTBPs));
                    tb.setLastSeen(now);
                    tb.setPeerID(peer);

                    tbs.put(peer, tb);
                }
            }
        }
        return tbs;
    }

    /**
     * Returns random peers not including the targetName peer.
     * @param num The number of random peers returned (at most).
     * @param targetName Not including this peer.
     * @return The random peers.
     */
    private Hashtable<Long, Long> getRandomPeers(int num, long targetName) {
        Hashtable<Long, Long> randomPeers = new Hashtable<Long, Long>(); // peer ID, long timestamp
        Long now = CommonState.getTime();

        /* We don't want more peers than available, let's pick some of them */
        if (num <= maxConnR) {
            ArrayList<Long> ids = new ArrayList<Long>(connR.keySet());
            ids.remove(targetName); /* Not including targetName */
            Collections.shuffle(ids, CommonState.r);
            Iterator i = ids.iterator();
            for (int n = 0; n < num && i.hasNext(); n++) {
                Long id = (Long) i.next();
                randomPeers.put(id, now);
            }
        } else { /* We want more peers than available, let's return them all */
            for (Long peer : connR.keySet()) { /* Copy all the peers */
                if (peer != targetName) { /* Not including targetName */
                    randomPeers.put(peer, now); /* Update last seen time */
                }
            }
        }
        return randomPeers;
    }

    private Collection randomSelectList(Collection ctb, int numTBs) {
        /* NOTE: this could be optimized */
        ArrayList tmp = new ArrayList(ctb);
        Deque result = new ArrayDeque();
        Collections.shuffle(tmp);
        for (int i = 0; i < numTBs && tmp.size() > 0; i++) {
            result.add(tmp.remove(0));
        }
        return result;
    }

    private void removeCandidate(long targetName) {
        candidates.remove(targetName);
    }

    private void closeConnection(long peerID) {
        if (connections.containsKey(peerID)) {
            updateLastSeen(peerID, CommonState.getTime());
            connections.remove(peerID);
            connT.remove(peerID);
            connR.remove(peerID);
            unconnT.remove(peerID);
        }
    }
// ======================== preference related======================
// =================================================================

    /**
     * Calculates the similarity between a peer and us.
     * @param peerID The peer's ID.
     * @return The similarity value.
     */
    private int getSimilarity(long peerID) {
        int sim = 0;
        Deque<Integer> peerPrefList = getPrefList(peerID);
        for (Integer pref : myPreferences) {
            if (peerPrefList.contains(pref)) {
                sim++;
            }
        }
        if (sim == 0) {
            return 0;
        }
        return (int) (1000 * (double) (sim) / Math.sqrt(myPreferences.size() * peerPrefList.size()));
    }

    /**
     * Fill the peer's own preferences from the outside.
     * @param prefs The preferences
     */
    public void addMyPreferences(Deque<Integer> prefs) {
        for (Integer item : prefs) {
            myPreferences.add(item);
        }
    }

    private Deque<Integer> getMyPreferences(int num) {
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

    Deque<Integer> getPrefList(long peerID) {
        return peerPreferences.get(peerID);
    }

    int addPreferences(long peerID, Deque<Integer> prefs) {
        int changed = 0;

        for (Integer item : prefs) {
            if (!peerPreferences.containsKey(peerID)) {
                peerPreferences.put(peerID,
                        new ArrayDeque<Integer>(maxPeerPreferences));
                /* NOTE: maximum storage size is specified here */
            }
            Deque<Integer> peerPrefList = peerPreferences.get(peerID);

            /* It is not on the peer's preference list, let's put it on */
            if (!peerPrefList.contains(item)) {
                if (!peerPrefList.offer(item)) {
                    /* The queue is full, we need to remove the first element */
                    peerPrefList.removeFirst();
                    boolean result = peerPrefList.offer(item);
                    assert (result == true);
                }
                changed = 1;
            }
        }

        if (changed == 1) {
            updateSimilarity(peerID);
        }
        return 0;
    }

    /**
     * Update the similarity values regarding an item.
     * @param item The item.
     */
    void updateAllSimilarity(int item) {
        /* See who has this item */
        for (Long peerID : peerPreferences.keySet()) {
            Deque<Integer> peerPrefList = peerPreferences.get(peerID);
            /* See if the item is on the list */
            if (peerPrefList.contains(item)) {
                /* Update the peer's preferences */
                updateSimilarity(peerID);
            }
        }
    }

    /**
     * Update a peer's similarity.
     * @param peerID The peer's ID.
     */
    private void updateSimilarity(long peerID) {
        if (idToSimilarity.containsKey(peerID)) {
            idToSimilarity.put(peerID, getSimilarity(peerID));
        }
    }
// ======================== helper classes =========================
// =================================================================

    /**
     * Singleton static Cycle Message class to keep the nodes running.
     */
    final static class CycleMessage {

        /**
         * The instance.
         */
        private static CycleMessage instance = null;

        /**
         * The constructor is private so it cannot be instantiated from the
         * outside.
         */
        private CycleMessage() {
        }

        /**
         * Returns the (one and only) CycleMessage instance. If there is none,
         * it creates one.
         * @return The CycleMessage instance.
         */
        public static CycleMessage getInstance() {
            if (instance == null) {
                instance = new CycleMessage();
            }
            return instance;
        }
    }

    /**
     * A not-so-general pair class representing a peer ID and a last seen time.
     */
    private final class IDTimePair implements Comparable {

        private final Long first, second;

        /**
         * Constructor.
         * @param first The peer ID.
         * @param second The last seen time.
         */
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
}
