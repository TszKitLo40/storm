package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.actormessage.IMessage;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;

/**
 * Created by robert on 4/7/16.
 */
public class ExecutionLatencyForRoutesMessage implements ITaskMessage {
    public int taskId;
    public ExecutionLatencyForRoutes latencyForRoutes;
    public ExecutionLatencyForRoutesMessage(int taskId, ExecutionLatencyForRoutes latencyForRoutes) {
        this.taskId = taskId;
        this.latencyForRoutes = latencyForRoutes;
    }
}
