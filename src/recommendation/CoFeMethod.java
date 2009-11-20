package recommendation;

import java.util.Map;
import java.util.TreeMap;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Node;

public class CoFeMethod implements CDProtocol{
    //------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------

    private static final String PAR_MEAN = "mean_value";
    //------------------------------------------------------------------------
    // Fields
    //------------------------------------------------------------------------
    protected final double mean;
    private Node node = null;
    private Map<Integer, Double> itemRates;
    private Map<Integer, Double> evalRates;
    private Map<Integer, Double> predictions;
    private double sumOfRates;
    private int numOfRatedItems;
    private int currentProtocolID;

    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    public CoFeMethod(String prefix) {
        mean = Configuration.getDouble(prefix + "." + PAR_MEAN);
        itemRates = new TreeMap<Integer, Double>();
        evalRates = new TreeMap<Integer, Double>();
        predictions = new TreeMap<Integer, Double>();
    }

    public Object clone() {
        CoFeMethod ret = null;
        try {
            ret = (CoFeMethod) super.clone();
            ret.node = node;
            ret.itemRates = new TreeMap<Integer, Double>();
            ret.evalRates = new TreeMap<Integer, Double>();
            ret.predictions = new TreeMap<Integer, Double>();
        } catch (CloneNotSupportedException e) {
            // never happens because of Protocol superclass
            e.printStackTrace();
        }
        return ret;
    }

    //------------------------------------------------------------------------
    // Methods
    //------------------------------------------------------------------------
    public double predicateItem(int itemID) {
        // get overlay
        Overlay overlay = getOverlay();

        // make prediction
        double prediction = 0.0;
        double aggregatedSimilarity = 0.0;
        boolean existsValue = false;

        for (int neighborIdx = 0; neighborIdx < overlay.getNumberOfNeighbors(); neighborIdx++) {
            // get rating of target item from neighbor
            Double neighborRate = getRate(overlay, neighborIdx, itemID);
            // get neighbor similarity
            double neighborSimilarity = getSimilarity(overlay, neighborIdx);
            // get the neighbor's mean of rates
            Double neighborMeanRate = getMeanRate(overlay, neighborIdx);

            // aggregate
            if (neighborRate != null) {
                existsValue = true;
                prediction += neighborSimilarity * (neighborRate - neighborMeanRate);
                aggregatedSimilarity += Math.abs(neighborSimilarity);
            }
        }

        // return prediction or the mean value of rating scale
        prediction /= aggregatedSimilarity;
        prediction += getMeanRate();
        return (existsValue && !Double.isNaN(prediction)) ? prediction : mean;
    }

    public void nextCycle(Node currentNode, int currentProtocolID) {
        this.currentProtocolID = currentProtocolID;

        // clear earlier predictions
        predictions.clear();

        // predicate
        for (int itemID : evalRates.keySet()) {
            double prediction = predicateItem(itemID);
            predictions.put(itemID, prediction);
        }
    }

    protected Overlay getOverlay() {
        // get the connection layer i.e. the overlay
        int linkableID = FastConfig.getLinkable(currentProtocolID);
        return (Overlay) node.getProtocol(linkableID);
    }

    protected Double getRate(Overlay overlay, int neighborIdx, int itemID) {
        NodeDescriptor desc = overlay.getDescriptor(neighborIdx);
        return desc.getRates().get(itemID);
    }

    protected Double getSimilarity(Overlay overlay, int neighborIdx) {
        return overlay.getSimilarity(neighborIdx);
    }

    protected Double getMeanRate(Overlay overlay, int neighborIdx) {
        return overlay.getDescriptor(neighborIdx).getMeanRate();
    }

    //------------------------------------------------------------------------
    // The following methods used for testing and initialization only!!!
    //------------------------------------------------------------------------
    public Double getMeanRate() {
        Double avg = (sumOfRates / ((double) numOfRatedItems));
        return (numOfRatedItems == 0 || Double.isNaN(avg)) ? null : avg;
    }

    public Map<Integer, Double> getRates() {
        return itemRates;
    }

    public Map<Integer, Double> getExpectedRates() {
        return this.evalRates;
    }

    public Map<Integer, Double> getPredictedRates() {
        return this.predictions;
    }

    public void addItemRate(int itemID, double rate) {
        itemRates.put(itemID, rate);
        sumOfRates += rate;
        numOfRatedItems++;
    }

    public void addEvalRate(int itemID, double rate) {
        evalRates.put(itemID, rate);
    }

    public void initializePredictions() {
        for (int itemID : evalRates.keySet()) {
            double prediction = predicateItem(itemID);
            predictions.put(itemID, prediction);
        }
    }

    public Double getItemRate(int itemID) {
        return itemRates.get(itemID);
    }

    public Double getEvalRate(int itemID) {
        return evalRates.get(itemID);
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}
