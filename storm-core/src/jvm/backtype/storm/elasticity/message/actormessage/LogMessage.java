package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 11/20/15.
 */
public class LogMessage implements IMessage{
    public String msg;
    public String host;
    public LogMessage(String msg, String host) {
        this.msg = msg;
        this.host = host;
    }
}
