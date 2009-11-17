package recommendation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;

public class OldNodeInitializer implements Control {
    //------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------
    private static final String PAR_APROT = "appprotocol";
    private static final String PAR_TRAINFILENAME = "train";
    private static final String PAR_EVALFILENAME = "eval";

    //------------------------------------------------------------------------
    // Fields
    //------------------------------------------------------------------------
    private final int apid;
    private final String trainFileName;
    private final String evalFileName;

    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    public OldNodeInitializer(String prefix) {
      apid = Configuration.getPid(prefix + "." + PAR_APROT);
      trainFileName = Configuration.getString(prefix + "." + PAR_TRAINFILENAME);
      evalFileName = Configuration.getString(prefix + "." + PAR_EVALFILENAME);
    }

    //------------------------------------------------------------------------
    // Methods
    //------------------------------------------------------------------------
    public boolean execute() {
      try {
        // read train
        readFile(trainFileName, true);
        readFile(evalFileName, false);
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
      return false;
    }
    
    private void readFile(String fileName, boolean isTrain) throws IOException {
      BufferedReader input = new BufferedReader(new FileReader(new File(fileName)));
      
      // process train file
      String line = input.readLine();
      while (line != null) {
        String[] values = line.split("\t");
        int userID = Integer.parseInt(values[0]) - 1;
        int itemID = Integer.parseInt(values[1]) - 1;
        double rating = Double.parseDouble(values[2]);
        
        // init node
        CoFeMethod usrnode = (CoFeMethod) Network.get(userID).getProtocol(apid);
        if (isTrain) {
          usrnode.addItemRate(itemID, rating);
        } else {
          usrnode.addEvalRate(itemID, rating);
        }
        
        // read next line
        line = input.readLine();
      }
      
      // close file
      input.close();
    }
}
