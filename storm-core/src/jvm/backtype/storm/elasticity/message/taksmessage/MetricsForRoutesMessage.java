package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.actormessage.IMessage;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;
import backtype.storm.elasticity.metrics.ThroughputForRoutes;

/**
 * Created by robert on 4/7/16.
 */
public class MetricsForRoutesMessage implements ITaskMessage {
    public int taskId;
    public ExecutionLatencyForRoutes latencyForRoutes;
    public ThroughputForRoutes throughputForRoutes;
    public String timeStamp;
    public MetricsForRoutesMessage(int taskId, ExecutionLatencyForRoutes latencyForRoutes) {
        this.taskId = taskId;
        this.latencyForRoutes = latencyForRoutes;
    }
    public MetricsForRoutesMessage(int taskId, ExecutionLatencyForRoutes latencyForRoutes, ThroughputForRoutes throughputForRoutes) {
        this.taskId = taskId;
        this.latencyForRoutes = latencyForRoutes;
        this.throughputForRoutes = throughputForRoutes;
    }

    public void setTimeStamp(String time) {
        this.timeStamp = time;
    }
}
