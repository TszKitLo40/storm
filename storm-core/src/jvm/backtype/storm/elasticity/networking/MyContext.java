package backtype.storm.elasticity.networking;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import java.util.Map;

/**
 * Created by robert on 12/8/15.
 */
public class MyContext implements IContext {


    @Override
    public void prepare(Map storm_conf) {

    }

    @Override
    public void term() {

    }

    @Override
    public IConnection bind(String storm_id, int port) {
        return new Receiver(port);
    }

    @Override
    public IConnection connect(String storm_id, String host, int port) {
        return new Sender(host, port);
    }
}
