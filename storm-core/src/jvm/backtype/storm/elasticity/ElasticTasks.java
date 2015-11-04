package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTasks {

    private RoutingTable routingTable;

    private HashMap<Integer, LinkedBlockingQueue<Tuple>> queues = new HashMap<>();

    public ElasticTasks(RoutingTable routing) {
        routingTable = routing;
        ArrayList<Integer> routes = routingTable.getRoutes();
        for(Integer i: routes) {
            queues.put(i, new LinkedBlockingQueue<Tuple>());
        }
    }


    public boolean handleTuple(Tuple tuple, Object key){
        int route = routingTable.route(key);
        if(route < 0)
            return false;
        try {
            queues.get(route).put(tuple);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return true;
        }

    }

    public ElasticTasks createHashRouting(int numberOfRoutes) {
        routingTable = new HashingRouting(numberOfRoutes);
        for(int i = 0; i < numberOfRoutes; i++)
            queues.put(i,new LinkedBlockingQueue<Tuple>());
        return this;
    }

}
