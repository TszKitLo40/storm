package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/18/15.
 */
public class BucketDistributionQueryCommand implements ICommand {
    public int taskid;
    public BucketDistributionQueryCommand(int taskid) {
        this.taskid = taskid;
    }
}
