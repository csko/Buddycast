package recommenderSystem.method;

import recommenderSystem.overlay.Overlay;

public class CoFeMethod extends AbstractMethod {
  
  public CoFeMethod(String prefix) {
    super(prefix);
  }
  
  @Override
  public double predicateItem(int itemID) {
    // get overlay
    Overlay overlay = getOverlay();
    
    // make prediction
    double prediction = 0.0;
    double aggregatedSimilarity = 0.0;
    boolean existsValue = false;
    
    for (int neighborIdx = 0; neighborIdx < overlay.getNumberOfNeighbors(); neighborIdx ++) {
      // get rating of target item from neighbor
      Double neighborRate = getRate(overlay, neighborIdx, itemID);
      // get neighbor similarity
      double neighborSimilarity = getSimilarity(overlay, neighborIdx);
      // get the neighbor's mean of rates
      Double neighborMeanRate = getMeanRate(overlay, neighborIdx);
      
      // aggregate
      if (neighborRate != null) {
        existsValue = true;
        prediction += neighborSimilarity *  (neighborRate - neighborMeanRate);
        aggregatedSimilarity += Math.abs(neighborSimilarity);
      }
    }
    
    // return prediction or the mean value of rating scale
    prediction /= aggregatedSimilarity;
    prediction += getMeanRate();
    return (existsValue && !Double.isNaN(prediction)) ? prediction :  mean;
  }
}
