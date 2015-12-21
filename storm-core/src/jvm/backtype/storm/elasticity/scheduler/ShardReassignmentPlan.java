package backtype.storm.elasticity.scheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 12/21/15.
 */
public class ShardReassignmentPlan {

    List<ShardReassignment> reassignmentList = new ArrayList<>();

    public void addReassignment(int taskId, int shardId, int originalRoute, int newRoute) {
        reassignmentList.add(new ShardReassignment(taskId, shardId, originalRoute, newRoute));
    }

    public List<ShardReassignment> getReassignmentList() {
        return reassignmentList;
    }

    public String toString() {
        String ret = "ShardReassignmentPlan: ";
        ret += reassignmentList.size() +" elements";
        if(reassignmentList.size()!=0) {
            for(ShardReassignment reassignment: reassignmentList) {
                ret += reassignment + "\n";
            }
        }
        return ret;
    }
}
