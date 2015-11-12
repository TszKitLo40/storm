package backtype.storm.elasticity;

import backtype.storm.elasticity.routing.HashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.VoidRouting;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTasks implements Serializable {

    private RoutingTable _routingTable;

    private BaseElasticBolt _bolt;

    private transient HashMap<Integer, LinkedBlockingQueue<Tuple>> _queues = new HashMap<>();

    private transient List<Thread> _queryThreads = new Vector<>();

    private transient List<QueryRunnable> _queryRunnables = new Vector<>();

    private transient ElasticOutputCollector _elasticOutputCollector;

    public ElasticTasks(BaseElasticBolt bolt) {
        _bolt = bolt;
        _routingTable = new VoidRouting();
    }

//    private ElasticTasks(BaseElasticBolt bolt, RoutingTable routing, ElasticOutputCollector elasticOutputCollector) {
//        _bolt = bolt;
//        _routingTable = routing;
//        _elasticOutputCollector = elasticOutputCollector;
//        ArrayList<Integer> routes = _routingTable.getRoutes();
//        for(Integer i: routes) {
//            _queues.put(i, new LinkedBlockingQueue<Tuple>());
//        }
//    }

    public void prepare(ElasticOutputCollector elasticOutputCollector) {
        _elasticOutputCollector = elasticOutputCollector;
    }

    public RoutingTable get_routingTable() {
        return _routingTable;
    }

    public synchronized boolean tryHandleTuple(Tuple tuple, Object key) {
        int route = _routingTable.route(key);
        if(route==RoutingTable.origin)
            return false;
        else {
            try {
                _queues.get(route).put(tuple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NullPointerException ne) {
                ne.printStackTrace();
                System.err.println("route:"+route+", queue size: "+_queues.size());
            }
            return true;
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
        ElasticTasks ret = new ElasticTasks(bolt);
        ret.prepare(collector);
        ret._routingTable = routingTable;
        ret.createAndLaunchElasticTasks();
////        ElasticTasks ret = new ElasticTasks(bolt, routingTable, collector);
//        for(int i : routingTable.getRoutes())
////        for(int i = 0; i < numberOfRoutes; i++)
//        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            ret._queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(bolt, inputQueue, collector);
//            ret._queryRunnables.add(query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            ret._queryThreads.add(newThread);
//        }
        return ret;
    }

    private void createAndLaunchElasticTasks() {
        for(int i : get_routingTable().getRoutes())
        {
            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
            _queues.put(i, inputQueue);

            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
            _queryRunnables.add(query);
            Thread newThread = new Thread(query);
            newThread.start();
            _queryThreads.add(newThread);
        }
    }

    public synchronized void setHashRouting(int numberOfRoutes) throws IllegalArgumentException {

        if(numberOfRoutes <= 0)
            throw new IllegalArgumentException("number of routes should be positive");

        if(!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
        }
        _routingTable = new HashingRouting(numberOfRoutes);
        createAndLaunchElasticTasks();
//        for(int i: _routingTable.getRoutes())
////        for(int i = 0; i < numberOfRoutes; i++)
//        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            _queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
//            _queryRunnables.add(query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            _queryThreads.add(newThread);
//        }
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
