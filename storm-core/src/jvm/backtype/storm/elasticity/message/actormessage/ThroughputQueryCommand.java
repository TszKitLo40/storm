package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/20/15.
 */
public class ThroughputQueryCommand implements ICommand {

    public int taskid;

    public ThroughputQueryCommand(int taskid) {
        this.taskid = taskid;
    }

}
