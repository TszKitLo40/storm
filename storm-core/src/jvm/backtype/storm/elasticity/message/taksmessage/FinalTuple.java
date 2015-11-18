package backtype.storm.elasticity.message.taksmessage;

/**
 * Created by Robert on 11/18/15.
 */
public class FinalTuple implements ITaskMessage {

    public int taskid;
    public int route;

    public FinalTuple(int taskid, int route) {
        this.taskid = taskid;
        this.route = route;
    }
}
