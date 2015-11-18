package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/17/15.
 */
public class RemoteRouteWithdrawCommand implements ICommand {

    public String host;
    public int taskId;
    public int route;

    public RemoteRouteWithdrawCommand(String host, int taskid, int route) {
        this.taskId = taskid;
        this.route = route;
        this.host = host;
    }

}
