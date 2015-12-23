package backtype.storm.elasticity.scheduler;

/**
 * Created by robert on 12/22/15.
 */
public class SubtaskReassignment {
    public String originalHost;
    public String targetHost;
    public int taskId;
    public int routeId;

    public SubtaskReassignment(String originalHost, String targetHost, int taskId, int routeId) {
        this.originalHost = originalHost;
        this.targetHost = targetHost;
        this.taskId = taskId;
        this.routeId = routeId;
    }

    public String toString() {
        String ret = "";
        ret += "Move " + taskId + "." + routeId + " from " + originalHost + " to " + targetHost;
        return ret;
    }
}
