package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/17/15.
 */
public class RoutingCreatingCommand implements ICommand {

    public int _task;
    public int _numberOfRoutes;
    public String _routingType;

    public RoutingCreatingCommand(int taskid, int numberOfroutes) {
        this(taskid, numberOfroutes, "hash");
    }

    public RoutingCreatingCommand(int taskid, int numberOfRoutes, String type) {
        _task = taskid;
        _numberOfRoutes = numberOfRoutes;
        _routingType = type;
    }

}
