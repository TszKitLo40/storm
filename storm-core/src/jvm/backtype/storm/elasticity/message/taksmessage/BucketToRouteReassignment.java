package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.actormessage.IMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/16/15.
 */
public class BucketToRouteReassignment implements IMessage{

    public int taskid;
    public Map<Integer, Integer> reassignment = new HashMap<>();

    public BucketToRouteReassignment(int taskId, int bucketid, int route) {
        this.taskid = taskId;
        reassignment.put(bucketid, route);
    }

    public BucketToRouteReassignment(int taskId, Map<Integer, Integer> reassignment) {
        this.reassignment.putAll(reassignment);
    }
}
