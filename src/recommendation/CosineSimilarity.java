package recommendation;

public class CosineSimilarity {

  public double computeSimilarity(AbstractMethod a, AbstractMethod b) {
    double sum = 0.0;
    double diva = 0.0;
    double divb = 0.0;
    for (int i : a.getRates().keySet()){
      if (b.getRates().containsKey(i)){
        sum += a.getRates().get(i) * b.getRates().get(i);
      }
    }
    if (sum > 0.0){
      for (int i : a.getRates().keySet()){
        diva += a.getRates().get(i) * a.getRates().get(i);
      }
      diva = Math.sqrt(diva);
      for (int i : b.getRates().keySet()){
        divb += b.getRates().get(i) * b.getRates().get(i);
      }
      divb = Math.sqrt(divb);
      sum /= diva * divb;
    }    
    return sum;
  }
}
