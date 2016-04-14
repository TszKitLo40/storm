package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 4/8/16.
 */
public class ExecutorScalingInRequestMessage implements IMessage {
    public int taskID;

    public ExecutorScalingInRequestMessage(int taskid) {
        taskID = taskid;
    }
}
