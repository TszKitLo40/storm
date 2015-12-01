package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/11/15.
 */
public class HelloMessage implements IMessage {

    String _name;

    int _port;

    public HelloMessage(String name) {
        _name = name;
    }

    public HelloMessage(String name, int port) {
        _name = name;
        _port = port;
    }

    public String getName() {
        return _name;
    }

    public int getPort() {
        return _port;
    }
}
