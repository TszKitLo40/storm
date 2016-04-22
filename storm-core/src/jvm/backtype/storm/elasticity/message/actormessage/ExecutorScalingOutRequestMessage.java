package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 4/8/16.
 */
public class ExecutorScalingOutRequestMessage implements IMessage {
    public int taskId;

    public ExecutorScalingOutRequestMessage(int taskId) {
        this.taskId = taskId;
    }
}
