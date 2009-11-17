package recommendation;

public class Overlay {

    public void initialzeDesriptors(int pid) {
    }

    public NodeDescriptor getDescriptor() {
        return null;
    }

    public NodeDescriptor getDescriptor(int idx) {
        /*        if (idx < buddies.size()) {
        for (Peers p : buddies) {
        if (idx == 0) {
        return p.getNodeDescriptor();
        }
        idx--;
        }
        }
        return null;
         */
        return null;
    }

    public double getSimilarity(int idx) {
        return 0.0;
    }

    public int getNumberOfNeighbors() {
        return 0;
    }
    private static Overlay instance = null;

    private Overlay() {
    }

    public static Overlay getInstance() {
        if (instance == null) {
            instance = new Overlay();
        }
        return instance;
    }
}
