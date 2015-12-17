package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/16/15.
 */
public class ReassignBucketToRouteCommand implements ICommand {

    public int taskId;
    public int bucketId;
    public int originalRoute;
    public int newRoute;

    public ReassignBucketToRouteCommand(int taskId, int bucketId, int originalRoute, int newRoute) {
        this.taskId = taskId;
        this.bucketId = bucketId;
        this.originalRoute = originalRoute;
        this.newRoute = newRoute;
    }
}
