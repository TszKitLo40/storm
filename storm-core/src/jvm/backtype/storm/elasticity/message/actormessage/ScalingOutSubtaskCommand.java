package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 1/27/16.
 */
public class ScalingOutSubtaskCommand implements ICommand {
    public int taskId;
    public ScalingOutSubtaskCommand(int taskId) {
        this.taskId = taskId;
    }
}
