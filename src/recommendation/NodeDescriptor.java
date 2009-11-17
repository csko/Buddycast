package recommendation;

import java.util.Map;

public class NodeDescriptor implements Cloneable {
  private int selections;
  private Map<Integer, Double> rates;
  private Double meanRate;
  
  public NodeDescriptor(int selections, Map<Integer, Double> rates, Double meanRate) {
    this.selections = selections;
    this.rates = rates;
    this.meanRate = meanRate;
  }
  
  public NodeDescriptor clone() {
    NodeDescriptor ret = new NodeDescriptor(selections, rates, meanRate);
    return ret;
  }
  
  public int getSelection() {
    return selections;
  }
  
  public Map<Integer, Double> getRates() {
    return rates;
  }
  
  public Double getMeanRate() {
    return meanRate;
  }
  
}
