package buddycast;

import peersim.vector.SingleValueHolder;
import peersim.config.*;
import peersim.core.*;
import peersim.transport.Transport;
import peersim.cdsim.CDProtocol;
import peersim.edsim.EDProtocol;

/*
 * Event driven version of the BuddyCast protocol.
 */
public class BuddyCast
implements EDProtocol {

    private String prefix;

    public BuddyCast( String prefix ) {
        this.prefix = prefix;
    }
/*
 * This is the standard method to define to process incoming messages.
 */
    public void processEvent( Node node, int pid, Object event ) {
        
    }

    public Object clone(){
        BuddyCast bc = null;
        try {
            bc = (BuddyCast) super.clone();
        } catch( CloneNotSupportedException e ) {} // never happens
        return bc;
    }
}
