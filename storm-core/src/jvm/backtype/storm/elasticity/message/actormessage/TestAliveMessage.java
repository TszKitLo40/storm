package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 1/21/16.
 */
public class TestAliveMessage implements IMessage {
    public String msg;
    public TestAliveMessage(String message) {
        msg = message;
    }
}
