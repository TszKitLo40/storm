package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 1/29/16.
 */
public class ScalingInSubtaskCommand implements ICommand {

    public int taskId;
    public ScalingInSubtaskCommand(int taskId) {
        this.taskId = taskId;
    }
}
