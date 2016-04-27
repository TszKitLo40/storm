package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.actormessage.IMessage;

/**
 * Created by robert on 26/4/16.
 */
public class PendingTupleCleanedMessage implements ITaskMessage {

    public int taskId;
    public int routeId;
    public PendingTupleCleanedMessage(int taskId, int routeId) {
        this.taskId = taskId;
        this.routeId = routeId;
    }
}
