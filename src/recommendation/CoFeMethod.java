package recommendation;

import buddycast.BuddyCast;
import java.util.Map;
import java.util.TreeMap;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Node;

public class CoFeMethod implements CDProtocol {
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
        // make prediction
        double prediction = 0.0;
        double aggregatedSimilarity = 0.0;
        boolean existsValue = false;

        int linkableID = FastConfig.getLinkable(currentProtocolID);
        BuddyCast bc = (BuddyCast) CommonState.getNode().getProtocol(linkableID);

        for (int i = 0; i < bc.degree(); i++) {
            Node neighborID = bc.getNeighbor(i);
            CoFeMethod method = (CoFeMethod) neighborID.getProtocol(
                    CommonState.getPid());

            // get rating of target item from neighbor
            Double neighborRate = method.getItemRate(itemID);
            // get neighbor similarity
            SimilarityMatrixFromFile sim = SimilarityMatrixFromFile.getInstance();
            double neighborSimilarity = sim.computeSimilarity((int) CommonState.getNode().getID(),
                    (int) neighborID.getID());
            // get the neighbor's mean of rates
            Double neighborMeanRate = method.getMeanRate();
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
}
