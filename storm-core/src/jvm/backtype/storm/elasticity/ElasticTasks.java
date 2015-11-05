package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;
import clojure.core.Vec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTasks {

    public RoutingTable _routingTable;

    private HashMap<Integer, LinkedBlockingQueue<Tuple>> _queues = new HashMap<>();

    private List<Thread> _queryThreads = new Vector<>();

    private List<QueryRunnable> _queryRunnables = new Vector<>();

    public ElasticTasks() {
        _routingTable = new VoidRouting();
    }

    public ElasticTasks(RoutingTable routing) {
        _routingTable = routing;
        ArrayList<Integer> routes = _routingTable.getRoutes();
        for(Integer i: routes) {
            _queues.put(i, new LinkedBlockingQueue<Tuple>());
        }
    }


    public boolean handleTuple(Tuple tuple, Object key){
        int route = _routingTable.route(key);
        if(route < 0)
            return false;
        try {
            _queues.get(route).put(tuple);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return true;
        }

    }


    public static ElasticTasks createHashRouting(int numberOfRoutes, BaseElasticBolt bolt, ElasticOutputCollector collector) {
        RoutingTable routingTable = new HashingRouting(numberOfRoutes);
        ElasticTasks ret = new ElasticTasks(routingTable);
        for(int i = 0; i < numberOfRoutes; i++) {
            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
            ret._queues.put(i, inputQueue);

            QueryRunnable query = new QueryRunnable(bolt, inputQueue, collector);
            ret._queryRunnables.add(query);
            Thread newThread = new Thread(query);
            newThread.start();
            ret._queryThreads.add(newThread);
        }
        return ret;
    }

    public void setHashRouting(int numberOfRoutes, BaseElasticBolt bolt, ElasticOutputCollector collector) throws IllegalArgumentException {

        if(numberOfRoutes <= 0)
            throw new IllegalArgumentException("number of routes should be positive");

        if(!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
        }
        _routingTable = new HashingRouting(numberOfRoutes);
        for(int i = 0; i < numberOfRoutes; i++) {
            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
            _queues.put(i, inputQueue);

            QueryRunnable query = new QueryRunnable(bolt, inputQueue, collector);
            _queryRunnables.add(query);
            Thread newThread = new Thread(query);
            newThread.start();
            _queryThreads.add(newThread);
        }
    }

    public void setVoidRounting() {
        if(!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
            _routingTable = new VoidRouting();
        }
    }

    private void terminateQueries() {
        for(QueryRunnable query: _queryRunnables) {
            query.terminate();
        }
        _queryRunnables.clear();
        for(Thread thread: _queryThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        }
        _queryThreads.clear();
        _queues.clear();
    }

}
