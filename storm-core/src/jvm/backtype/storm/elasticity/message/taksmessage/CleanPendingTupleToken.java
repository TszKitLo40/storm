package backtype.storm.elasticity.message.taksmessage;


/**
 * Created by robert on 26/4/16.
 */
public class CleanPendingTupleToken implements ITaskMessage {
    public int taskId;
    public int routeId;
    public CleanPendingTupleToken(int taskid, int routeid) {
        taskId = taskid;
        routeId = routeid;
    }
}
