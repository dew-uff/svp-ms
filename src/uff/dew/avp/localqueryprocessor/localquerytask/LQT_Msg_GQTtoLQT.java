package uff.dew.avp.localqueryprocessor.localquerytask;

/**
 *
 * @author Luiz Matos
 */
public class LQT_Msg_GQTtoLQT extends LQT_Message {
    
    //private int a_idSlave; // id of the LQT getting interval 
    private String a_xquery; // query which will be processed
    private int a_initialPosition; // initial position of the interval
    private int a_endPosition; // end position of the interval
    
    /** Creates a new instance of LQT_Msg_CQPtoLQT */
    public LQT_Msg_GQTtoLQT( int idSlave, String xquery, int initialPosition, int endPosition ) {
        super( idSlave );
    	a_xquery = xquery;		
    	a_initialPosition = initialPosition;
    	a_endPosition = endPosition;
    }
    
    public String getXQuery() {
        return a_xquery;
    }
    
    public int getInitialPosition() {
        return a_initialPosition;
    }
    
    public int getEndPosition() {
        return a_endPosition;
    }
    
    public int getType() {
        return MSG_GQTTOLQT;
    }
}
