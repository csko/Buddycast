package buddycast;

import java.util.*;
import peersim.config.FastConfig;
import peersim.core.*;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;
import recommendation.SimilarityMatrixFromFile;

/**
 * Event driven implementation of the BuddyCast protocol.
 */
public class BuddyCast
        implements EDProtocol, Linkable {

    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------
    private static final String PAR_MAXCONNT = "maxconnt";
    private static final String PAR_MAXCONNR = "maxconnr";
    private static final String PAR_MAXUNCONNT = "maxuconnt";
    private static final String PAR_MAXCANDIDATES = "maxcandidates";
    private static final String PAR_MAXPEERPREFS = "maxpeerprefs";
    private static final String PAR_DELAY = "delay";
    private static final String PAR_ALPHA = "alpha";
    private static final String PAR_BLOCKINTERVAL = "blockinterval";
    private static final String PAR_NUMMSGMYPREFS = "nummsgmyprefs";
    private static final String PAR_NUMMSGRANDOMPEERS = "nummsgrandompeers";
    private static final String PAR_NUMMSGTASTEBUDDIES = "nummsgtastebuddies";
    private static final String PAR_NUMMSGTBPREFS = "nummsgtbprefs";
    private static final String PAR_TIMEOUT = "timeout";
    private static final String PAR_PRESERVEMEMORY = "preservememory";

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
    private static ArrayList<Node> superpeers = new ArrayList<Node>();

    /**
     * Create superpeers.
     */
    static {
        for (int i = 0; i < numSuperPeers; i++) {
            // TODO: test this
            // NOTE: this does not work very well with network shuffle
            superpeers.add(Network.get(i));
        }
    }
    /**
     * Use recommendation related similarity function?
     */
    private static boolean recommendation = true;
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
    private Hashtable<Node, Double> nodeToSimilarity;
    private Hashtable<Node, Long> nodeToConnTime;
    /* List of active TCP connections */
    Hashtable<Node, Long> connections; // Peer ID, keep-alivetimeout
    /**
     * These three containers make up the Connection List C_C.
     */
    /* Connectible taste buddies */
    Hashtable<Node, Long> connT; // Peer ID, last seen
    /* Connectible random peers */
    Hashtable<Node, Long> connR; // Peer ID, last seen
    /* Unconnectible taste buddies */
    Hashtable<Node, Long> unconnT; // Peer ID, last seen
    /* Connection Candidates */
    Hashtable<Node, Long> candidates; // Peer ID, timestamp
    /**
     * Block lists.
     */
    Hashtable<Node, Long> recvBlockList; // Peer ID, timestamp
    Hashtable<Node, Long> sendBlockList; // Peer ID, timestamp
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
    Hashtable<Node, Deque<Integer>> peerPreferences;
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
     * 0 means no limit.
     */
    private final int maxPeerPrefs = 500;
// ======================== message and behavior ===================
// =================================================================
    /**
     * Time to wait between two active thread activizations.
     */
    static final long delay = 15;
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
// ============================ memory =============================
// =================================================================
    private boolean preserveMemory = false;
    final int initialCapacity = Math.min(5, maxConnT);
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
        if (preserveMemory) {
            nodeToSimilarity = new Hashtable<Node, Double>();
            nodeToConnTime = new Hashtable<Node, Long>();
            connections = new Hashtable<Node, Long>(initialCapacity);
            connT = new Hashtable<Node, Long>(initialCapacity);
            connR = new Hashtable<Node, Long>(initialCapacity);
            unconnT = new Hashtable<Node, Long>(initialCapacity);
            candidates = new Hashtable<Node, Long>(initialCapacity);
            recvBlockList = new Hashtable<Node, Long>();
            sendBlockList = new Hashtable<Node, Long>();
            myPreferences = new ArrayDeque<Integer>(numMsgMyPrefs);
            peerPreferences = new Hashtable<Node, Deque<Integer>>();
        } else {
            nodeToSimilarity = new Hashtable<Node, Double>();
            nodeToConnTime = new Hashtable<Node, Long>();
            connections = new Hashtable<Node, Long>(maxConnT + maxConnR);
            connT = new Hashtable<Node, Long>(maxConnT);
            connR = new Hashtable<Node, Long>(maxConnR);
            unconnT = new Hashtable<Node, Long>(maxUnConnT);
            candidates = new Hashtable<Node, Long>(maxCandidates);
            recvBlockList = new Hashtable<Node, Long>();
            sendBlockList = new Hashtable<Node, Long>();
            myPreferences = new ArrayDeque<Integer>(numMsgMyPrefs);
            peerPreferences = new Hashtable<Node, Deque<Integer>>();
        }
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
        Collection<Node> values = connections.keySet();
        Object[] valuesArray = values.toArray();

        assert (valuesArray.length == degree());

        return (Node) valuesArray[i];

    }

    public boolean addNeighbor(Node node) {
        /* Also known as addConnection() */
        if (contains(node)) {
            return false;
        }

        addPeer(node);
        Long now = CommonState.getTime();
        updateLastSeen(node, now);
        connections.put(node, now + timeout);
        if (useInit) {
            /* Get the peer's items as if they were sent in a message */
            addPeerToConnList(node, true); // TODO: always connectible
            // TODO: uncomment this line
            if (!recommendation) {
                addPreferences(node, ((BuddyCast) node.getProtocol(protocolID)).getMyPreferences(numMsgMyPrefs));
            }
            addConnCandidate(node, now);
        }
        return true;
    }

    /* NOTE: this is not a method of Linkable but fits here logically. */
    /**
     * Remove a neighbor.
     * @param peerID The neighbor peer's ID.
     * @param symmre true, if there is a need to remove the other edge too
     */
    private void removeNeighbor(Node node, boolean symmetric) {

        if (connections.containsKey(node)) {
            updateLastSeen(node, CommonState.getTime());
            connections.remove(node);

            connT.remove(node);
            connR.remove(node);
            unconnT.remove(node);
        }

        if (symmetric) {
            /* Switch to the neighbor node and remove the link
             * NOTE: This is possibly a hack.
             */
            Node myNode = CommonState.getNode();
            CommonState.setNode(node);
            ((BuddyCast) node.getProtocol(protocolID)).removeNeighbor(node, false);
            CommonState.setNode(myNode);
        }
    }

    public boolean contains(Node node) {
        if (connections.containsKey(node)) {
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
        if (event instanceof CycleMessage) {
            /* Cycle message, schedule an other message in timeToWait time */
            EDSimulator.add(delay, CycleMessage.getInstance(), node, pid);

            /* Do the active BuddyCast protocol */
            work(pid);
//            System.out.println(CommonState.getNode().getID() + " " + CommonState.getTime());
        } else if (event instanceof BuddyCastMessage) {
            //System.out.println("BuddyCastMessage!");
            /* Handle incoming BuddyCast message */
            BuddyCastMessage msg = (BuddyCastMessage) event;

            /* See if the peer is blocked */
            if (isBlocked(msg.sender, recvBlockList)) {
                return;
            }

            // NOTE: This is load related statistics
            incrementSelection();

            // TODO: see if the peer is on our connections list?

            int changed = 0;
            changed += addPreferences(msg.sender, msg.myPrefs);

            /* Use the Taste Buddy list provided in the message */
            for (TasteBuddy tb : msg.tasteBuddies.values()) {
                if (addPeer(tb.getNode()) == 1) { /* Peer newly added */
                    updateLastSeen(tb.getNode(), tb.getLastSeen());
                }
                changed += addPreferences(tb.getNode(), tb.getPrefs());
                addConnCandidate(tb.getNode(), tb.getLastSeen());
            }

            /* Use the Random Peer list provided in the message */
            for (Node peer : msg.randomPeers.keySet()) {
                if (addPeer(peer) == 1) { /* Peer newly added */
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
                    replyMsg.sender = CommonState.getNode();
                    /* It's a reply */
                    replyMsg.reply = true;
                    /* Send the message */
                    Node senderNode = msg.sender;
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
        Node peer = tasteSelectTarget(alpha);
        if (peer == null) {
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
            msg.sender = CommonState.getNode();
            /* Not a reply */
            msg.reply = false;
            /* Send it */
            ((Transport) peer.getProtocol(FastConfig.getTransport(pid))).send(
                    CommonState.getNode(),
                    peer,
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
        for (Node peerID : superpeers) {
            /* Don't add myself (as a superpeer) */
            if (peerID != CommonState.getNode()) {
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
        Hashtable<Node, Long> superPeerList = new Hashtable<Node, Long>();
        for (Node peerID : nodeToConnTime.keySet()) {
            /* See if the peer is blocked */
            if (!isBlocked(peerID, sendBlockList)) {
                superPeerList.put(peerID, nodeToConnTime.get(peerID));
            }
        }
        /* Add the most recent peers as candidates */
        ArrayList<Node> recentPeers = selectRecentPeers(superPeerList, numSuperPeers);
        for (Node peerID : recentPeers) {
            addConnCandidate(peerID, nodeToConnTime.get(peerID));
        }
    }

    private BuddyCastMessage createBuddyCastMessage(Node targetName) {
        BuddyCastMessage msg = new BuddyCastMessage();
        /* NOTE: When doing a recommendation, we are sending the whole item list */
        if (recommendation) {
            msg.myPrefs = getMyPreferences(0);
        } else {
            msg.myPrefs = getMyPreferences(numMsgMyPrefs);
        }
        msg.tasteBuddies = getTasteBuddies(numMsgTasteBuddies, numMsgTBPrefs, targetName);
        msg.randomPeers = getRandomPeers(numMsgRandomPeers, targetName);
        msg.connectible = connectible;
        return msg;
    }

    private int connectPeer(Node node) {
        /* TODO: Don't always successfully connect to the peer */
        int result = 0; /* The connection was successful */

        assert (!isBlocked(node, sendBlockList));

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
    private Node tasteSelectTarget(double alpha) {
        Node targetName = null;
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

    public void setInit(boolean init) {
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
    private void addConnCandidate(Node peerID, Long lastSeen) {
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
            // TODO: linear search is a performance bottleneck here
            /* Find the oldest entry */
            Node oldestPeer = null;
            long oldestPeerTime = Long.MAX_VALUE;
            for (Node peer : candidates.keySet()) {
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
    private Node addNewPeerToConnList(Hashtable<Node, Long> connList,
            int maxNum, Node peerID, Long connTime) {
        Node oldestPeerID = null;
        long oldestPeerTime = connTime + 1;

        /* The list is not full, we don't have to remove */
        if (connList.size() < maxNum) {
            connList.put(peerID, connTime);
            return oldestPeerID; /* none removed */
        } else {
            /* Get the oldest peer */
            for (Node peer : connList.keySet()) {
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
    int addPeer(Node peerID) {
        /* Check if the peer is myself */
//        if (peerID == CommonState.getNode().getID()) {
//            System.exit(1);
//            return -1;
//        }

        /* TODO: this might need some refactoring */
        if (!nodeToConnTime.containsKey(peerID) &&
                !nodeToSimilarity.containsKey(peerID)) {
            nodeToConnTime.put(peerID, new Long(0));
            nodeToSimilarity.put(peerID, new Double(0));
            return 1;
        }
        return 0;
    }

    private void addPeerToConnList(Node peerID, boolean connectible) {
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
    boolean addPeerToConnT(Node peerID, Long now) {
        if (connT.containsKey(peerID)) { /* Peer is already on the list */
            return true;
        }

        //double sim = nodeToSimilarity.get(peerID);
        // TODO: need to call updateSimilarities() so nodeToSimilarity gets updated
        double sim = getSimilarity(peerID);

        if (sim > 0) {
            /* The list is not full, we don't have to remove */
            if (connT.size() < maxConnT) {
                connT.put(peerID, now);
                return true;
            } else { /* The list is full, we need to remove the least similar peer */
                /* Get the peer with minimal similarity */
                Long minPeerTime = now + 1;
                Node minPeerID = null;
                double minSim = Double.POSITIVE_INFINITY;

                for (Node peer : connT.keySet()) {
                    Long peerTime = connT.get(peer);
                    //double peerSim = nodeToSimilarity.get(peer);
                    // TODO: same as above
                    double peerSim = getSimilarity(peer);

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

    private void addPeerToConnR(Node peerID, Long now) {
        if (!connR.contains(peerID)) {
            Node outPeer = addNewPeerToConnList(connR, maxConnR, peerID, now);
            if (outPeer != null) {
                removeNeighbor(outPeer, true);
            }
        }
    }

    private void addPeerToUnConnT(Node peerID, Long now) {
        if (!unconnT.contains(peerID)) {
            Node outPeer = addNewPeerToConnList(unconnT, maxUnConnT, peerID, now);
            if (outPeer != null) {
                removeNeighbor(outPeer, true);
            }
        }
    }

    private void blockPeer(Node peerID, Hashtable<Node, Long> list) {
        assert (peerID != null);
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

    private void updateLastSeen(Node peerID, Long lastSeen) {
        if (nodeToConnTime.containsKey(peerID)) {
            nodeToConnTime.put(peerID, lastSeen);
        }
    }

    /**
     * Determines if a peer is on the block list.
     * @param peerID The peer's ID.
     * @param list The list to examine.
     * @return true, if the peer is on the list; false otherwise
     */
    private boolean isBlocked(Node peerID, Hashtable<Node, Long> list) {
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
    private Node selectTasteBuddy() {
        Node maxId = null; /* The buddy Node */
        double maxSimilarity = Double.NEGATIVE_INFINITY; /* The similarity of the buddy */
        for (Node peer : candidates.keySet()) {
            //Double similarity = nodeToSimilarity.get(peer);
            // TODO: update similarities, etc
            Double similarity = getSimilarity(peer);

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
    private Node selectRandomPeer() {
        long i = 0;
        int r = CommonState.r.nextInt(candidates.size());
        for (Node peer : candidates.keySet()) {
            if (i == r) {
                return peer;
            }
            i++;
        }
        return null;
    }

    /**
     * Selects the last few number of peers according to the last seen time.
     * @param list The list to select peers from.
     * @param number The number of peers to select.
     * @return The peer list.
     */
    public ArrayList<Node> selectRecentPeers(Hashtable<Node, Long> list, int number) {
        ArrayList<Node> ret = new ArrayList<Node>(); // the resulting peer IDs
        ArrayList<IDTimePair> peers = new ArrayList<IDTimePair>();
        for (Node peerID : list.keySet()) {
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

    private Hashtable<Node, TasteBuddy> getTasteBuddies(
            int numTBs, int numTBPs, Node targetName) {
        Hashtable<Node, TasteBuddy> tbs = new Hashtable<Node, TasteBuddy>();
        Long now = CommonState.getTime();

        if (numTBs < numMsgTasteBuddies) {
            Vector<Node> ctb = new Vector<Node>(); // Connected taste buddies
            for (Node peer : connT.keySet()) {
                ctb.add(peer);
            }
            Deque<Node> tb_list = (Deque<Node>) randomSelectList(ctb, numTBs);
            for (Node peer : tb_list) {
                /* Not including the target */
                if (!peer.equals(targetName)) {
                    /* Set up the taste buddy */
                    TasteBuddy tb = new TasteBuddy();
                    tb.setPrefs(
                            randomSelectItems(
                            (ArrayDeque<Integer>) peerPreferences.get(peer),
                            numTBPs));
                    tb.setLastSeen(now);
                    tb.setNode(peer);

                    tbs.put(peer, tb);
                }
            }
        } else {
            for (Node peer : connT.keySet()) {
                /* Not including the target */
                if (!peer.equals(targetName)) {
                    /* Set up the taste buddy */
                    TasteBuddy tb = new TasteBuddy();
                    Deque<Integer> peerPrefs = peerPreferences.get(peer);
                    if (peerPrefs == null) { // TODO: this shouldn't happen
                        /* NOTE: When doing a recommendation, we are using a precalculated similarity function */
                        if (recommendation) {
                            peerPrefs = new ArrayDeque<Integer>(1);
                        } else {
                            peerPrefs = new ArrayDeque<Integer>(maxPeerPrefs);
                            addPreferences(peer, peerPrefs);
                        }
                    }

                    tb.setPrefs(
                            randomSelectItems(
                            (ArrayDeque<Integer>) peerPrefs, numTBPs));
                    tb.setLastSeen(now);
                    tb.setNode(peer);

                    tbs.put(peer, tb);
                }
            }
        }
        return tbs;
    }

    public Hashtable<Node, Long> getConnections() {
        return connections;
    }

    public Hashtable<Node, Long> getConnT() {
        return connT;
    }

    public Hashtable<Node, Long> getConnR() {
        return connR;
    }

    public Hashtable<Node, Long> getUnConnT() {
        return unconnT;
    }

    /**
     * Returns random peers not including the targetName peer.
     * @param num The number of random peers returned (at most).
     * @param targetName Not including this peer.
     * @return The random peers.
     */
    private Hashtable<Node, Long> getRandomPeers(int num, Node targetName) {
        Hashtable<Node, Long> randomPeers = new Hashtable<Node, Long>(); // peer ID, long timestamp
        Long now = CommonState.getTime();

        /* We don't want more peers than available, let's pick some of them */
        if (num <= maxConnR) {
            ArrayList<Node> ids = new ArrayList<Node>(connR.keySet());
            ids.remove(targetName); /* Not including targetName */
            Collections.shuffle(ids, CommonState.r);
            Iterator i = ids.iterator();
            for (int n = 0; n < num && i.hasNext(); n++) {
                Node id = (Node) i.next();
                randomPeers.put(id, now);
            }
        } else { /* We want more peers than available, let's return them all */
            for (Node peer : connR.keySet()) { /* Copy all the peers */
                if (peer != targetName) { /* Not including targetName */
                    randomPeers.put(peer, now); /* Update last seen time */
                }
            }
        }
        return randomPeers;
    }

    private Collection randomSelectList(Collection ctb, int numTBs) {
        /* NOTE: this should be optimized */
        ArrayList tmp = new ArrayList(ctb);
        Deque result = new ArrayDeque();
        Collections.shuffle(tmp, CommonState.r);
        for (int i = 0; i < numTBs && tmp.size() > 0; i++) {
            result.add(tmp.remove(0));
        }
        return result;
    }

    private Deque<Integer> randomSelectItems(ArrayDeque<Integer> ctb, int numTBs) {
        /* NOTE: this should be optimized */
        //ArrayDeque<Integer> tmp = ctb.clone();
        ArrayDeque<Integer> result = new ArrayDeque<Integer>();
        ArrayList<Integer> tmpList = new ArrayList<Integer>(ctb);
        Collections.shuffle(tmpList, CommonState.r);
        for (int i = 0; i < numTBs && tmpList.size() > 0; i++) {
            result.add(tmpList.remove(0));
        }
        return result;
    }

    private void removeCandidate(Node targetName) {
        candidates.remove(targetName);
    }

// ======================== preference related======================
// =================================================================
    /**
     * Calculates the similarity between a peer and us.
     * @param peerID The peer's ID.
     * @return The similarity value.
     */
    private double getSimilarity(Node peerID) {
        /* NOTE: this is recommendation specific */
        if (recommendation) {
            SimilarityMatrixFromFile sim = SimilarityMatrixFromFile.getInstance();
            return sim.computeSimilarity((int) CommonState.getNode().getID(), (int) peerID.getID());
        }
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

    /**
     * Return a number of most recent preferences.
     * @param num Number of preferences.
     * @return The preferences.
     */
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

    /**
     * Return a peer's stored preference list.
     * @param peerID The peer.
     * @return The preference list.
     */
    Deque<Integer> getPrefList(Node peerID) {
        return peerPreferences.get(peerID);
    }

    int addPreferences(Node peerID, Deque<Integer> prefs) {
        /* NOTE: When doing a recommendation, we use a precalculated similarity function */
        if (recommendation) {
            if (!peerPreferences.containsKey(peerID)) {
                peerPreferences.put(peerID,
                        new ArrayDeque<Integer>(0));
            }
            return 0;

        }

        int changed = 0;

        if (!peerPreferences.containsKey(peerID)) {
            peerPreferences.put(peerID,
                    new ArrayDeque<Integer>(maxPeerPrefs));
            /* NOTE: maximum storage size is specified here */
        }

        Deque<Integer> peerPrefList = peerPreferences.get(peerID);

        for (Integer item : prefs) {
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
        for (Node peerID : peerPreferences.keySet()) {
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
    private void updateSimilarity(Node peerID) {
        if (nodeToSimilarity.containsKey(peerID)) {
            nodeToSimilarity.put(peerID, getSimilarity(peerID));
        }
    }
// ======================== helper classes =========================
// =================================================================

    /**
     * Singleton static Cycle Message class to keep the nodes running.
     */
    public final static class CycleMessage {

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
     * A not-so-general pair class representing a Node and a last seen time.
     */
    private final class IDTimePair implements Comparable {

        private final Node first;
        private final Long second;

        /**
         * Constructor.
         * @param first The peer ID.
         * @param second The last seen time.
         */
        IDTimePair(Node first, Long second) {
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
// ========================== statistics ===========================
// =================================================================
    private int selection = 0;

    public int getSelection() {
        return selection;
    }

    public void incrementSelection() {
        this.selection++;
    }

    public void initSelection() {
        this.selection = 0;
    }

    public int getBlockSize() {
        HashSet<Node> blockedPeers = new HashSet<Node>();
        for (Node peer : recvBlockList.keySet()) {
            blockedPeers.add(peer);
        }
        for (Node peer : sendBlockList.keySet()) {
            blockedPeers.add(peer);
        }

        return blockedPeers.size();

    }
}
