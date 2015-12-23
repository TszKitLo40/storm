package backtype.storm.elasticity.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by robert on 12/22/15.
 */
public class SubtaskReassignmentPlan {

    List<SubtaskReassignment> subtaskReassignments = new ArrayList<>();

    public void addSubtaskReassignment(String originalHost, String targetHost, int taskId, int route) {
        subtaskReassignments.add(new SubtaskReassignment(originalHost, targetHost, taskId, route));
    }

    public List<SubtaskReassignment> getSubTaskReassignments() {
        return subtaskReassignments;
    }

    public String toString() {
        String ret = "";
        ret += "Subtask Reassignment Plan:\n";
        for(SubtaskReassignment reassignment: subtaskReassignments) {
            ret += reassignment.toString() + "\n";
        }
        return ret;

    }

    public void concat(SubtaskReassignmentPlan plan) {
        Set<String> existingTaskRoutes = getTaskRoutes();
        for(SubtaskReassignment reassignment: plan.getSubTaskReassignments()) {
            String subtaskRoute = reassignment.taskId + "." + reassignment.routeId;
            if(existingTaskRoutes.contains(subtaskRoute)) {
                System.err.println(subtaskRoute + " already exists! This movement will be ignored!");
                continue;
            }
            subtaskReassignments.add(reassignment);
        }
    }

    private Set<String> getTaskRoutes() {
        HashSet<String> ret = new HashSet<>();
        for(SubtaskReassignment reassignment: subtaskReassignments) {
            ret.add(reassignment.taskId + "." + reassignment.routeId);
        }
        return ret;
    }

}
