package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/16/15.
 */
public class RoutingTableQueryCommand implements ICommand{

    public int taskid;

    public boolean completeRouting = true;

    public RoutingTableQueryCommand(int taskid) {
        this.taskid = taskid;
    }

}
