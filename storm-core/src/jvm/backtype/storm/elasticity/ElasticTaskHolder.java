package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.util.HashMap;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    private HashMap<Integer,ElasticTasks> tasks;

    /**
     * This function test whether a tuple should be processed by the elastic tasks. If yes, insert
     * the tuple to the pending buffer and return true; else return false.
     * @param tuple the input tuple
     * @param taskId the task id
     * @param bolt the bolt
     * @return true if the tuple should be processed by the elastic tasks.
     */
    public boolean handledByElasticTasks(Tuple tuple, int taskId, BaseElasticBolt bolt) {
        if(!tasks.containsKey(taskId))
            return false;
        ElasticTasks elasticTask = tasks.get(taskId);
        tuple.getValueByField("abc");
        return elasticTask.handleTuple(tuple,bolt.getKey(tuple));
    }

    public void createElasticTasks(int originalTaskId, RoutingTable routingTable) {
        tasks.put(originalTaskId, new ElasticTasks(routingTable));
    }
}
