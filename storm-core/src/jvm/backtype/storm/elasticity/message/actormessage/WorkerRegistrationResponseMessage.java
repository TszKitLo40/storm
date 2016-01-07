package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/31/15.
 */
public class WorkerRegistrationResponseMessage implements IMessage {

    public String ip;
    public int port;
    public String masterIp;

    public WorkerRegistrationResponseMessage(String masterIp, String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.masterIp = masterIp;
    }

    public String toString() {
        return ip + ":" + port;
    }

}
