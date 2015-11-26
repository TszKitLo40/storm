package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 11/25/15.
 */
public class DistributionQueryCommand implements ICommand {

    public int taskid;
    public DistributionQueryCommand(int taskid) {
        this.taskid = taskid;
    }
}
