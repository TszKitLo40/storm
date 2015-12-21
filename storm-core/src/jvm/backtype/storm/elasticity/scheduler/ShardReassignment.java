package backtype.storm.elasticity.scheduler;

/**
 * Created by robert on 12/21/15.
 */
public class ShardReassignment {
    public int taskId;
    public int shardId;
    public int originalRoute;
    public int newRoute;

    public ShardReassignment(int taskId, int shardId, int originalRoute, int newRoute) {
        this.taskId = taskId;
        this.shardId = shardId;
        this.originalRoute = originalRoute;
        this.newRoute = newRoute;
    }

    public String toString() {
        String ret = "";
        ret += "Reassign " + "task " + taskId + " shard " + shardId + " from " + originalRoute + " to " + newRoute;
        return ret;
    }
}
