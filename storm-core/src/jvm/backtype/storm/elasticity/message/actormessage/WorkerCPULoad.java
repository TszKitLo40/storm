package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/22/15.
 */
public class WorkerCPULoad implements IMessage {
    public double cpuLoad;
    public String hostName;

    public WorkerCPULoad(String hostName, double cpuLoad) {
        this.cpuLoad = cpuLoad;
        this.hostName = hostName;
    }

}
