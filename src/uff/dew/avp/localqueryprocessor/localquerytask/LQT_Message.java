package uff.dew.avp.localqueryprocessor.localquerytask;

/**
 *
 * @author  Alex
 */

abstract public class LQT_Message {

    // Message types
    static public final int MSG_HELPOFFER = 0;
    static public final int MSG_HELPNOTACCEPTED = 1;
    static public final int MSG_INTERVALFINISHED = 2;
    static public final int MSG_FINISH = 3;
    static public final int MSG_HELPACCEPTED = 4;
    static public final int MSG_GQTTOLQT = 5;
    
    private int a_idSender;

    /** Creates a new instance of LQT_Message */
    public LQT_Message( int idSender ) {
	a_idSender = idSender;
    }

    abstract public int getType();

    public int getIdSender() {
	return a_idSender;
    }
}
