package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/22/15.
 */
public class WorkerCPULoad implements IMessage {
    public double processCpuLoad;
    public double systemCpuLoad;
    public String hostName;

    public WorkerCPULoad(String hostName, double processCpuLoad) {
        this.processCpuLoad = processCpuLoad;
        this.hostName = hostName;
    }

    public WorkerCPULoad(String hostName, double processCpuLoad, double systemCpuLoad) {
        this.processCpuLoad = processCpuLoad;
        this.systemCpuLoad = systemCpuLoad;
        this.hostName = hostName;
    }

}
