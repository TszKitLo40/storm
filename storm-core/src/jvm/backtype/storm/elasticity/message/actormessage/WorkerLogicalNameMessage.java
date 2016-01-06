package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/31/15.
 */
public class WorkerLogicalNameMessage implements IMessage {

    public String ip;
    public int port;

    public WorkerLogicalNameMessage(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String toString() {
        return ip + ":" + port;
    }

}
