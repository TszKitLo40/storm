package backtype.storm.elasticity;

import backtype.storm.elasticity.routing.HashingRouting;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.VoidRouting;
import backtype.storm.tuple.Tuple;
import org.apache.logging.log4j.core.appender.routing.Route;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTasks implements Serializable {

    private RoutingTable _routingTable;

    private BaseElasticBolt _bolt;

    private int _taskID;
    private transient HashMap<Integer, LinkedBlockingQueue<Tuple>> _queues;

    private transient HashMap<Integer, Thread> _queryThreads;

    private transient HashMap<Integer, QueryRunnable> _queryRunnables;

    private transient ElasticOutputCollector _elasticOutputCollector;

    private transient LinkedBlockingQueue<ITaskMessage> _remoteTupleQueue;

    public ElasticTasks(BaseElasticBolt bolt, Integer taskID) {
        _bolt = bolt;
        _taskID = taskID;
        _routingTable = new VoidRouting();
    }

//    public ElasticTasks(ElasticTasks instance) {
//        _bolt = instance._bolt;
//        _taskID = instance._taskID;
//        _routingTable = new VoidRouting();
//    }

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
        _queues = new HashMap<>();
        _queryThreads = new HashMap<>();
        _queryRunnables = new HashMap<>();
        _elasticOutputCollector = elasticOutputCollector;
    }

    public RoutingTable get_routingTable() {
        return _routingTable;
    }

    public void set_routingTable(RoutingTable routingTable) {
        _routingTable = routingTable;
    }

    public synchronized boolean tryHandleTuple(Tuple tuple, Object key) {
        int route = _routingTable.route(key);
        if(route==RoutingTable.origin)
            return false;
        else if (route == RoutingTable.remote) {
            System.out.println("a tuple is routed to remote!");

            RemoteTuple remoteTuple = new RemoteTuple(_taskID, ((PartialHashingRouting)_routingTable).getOrignalRoute(key), tuple);
            try {
                _remoteTupleQueue.put(remoteTuple);
                System.out.println("Remote Tuple is generated and sent to _remoteTupleQueue");
            } catch (InterruptedException e) {
                e.printStackTrace();

            }
            return true;
        }
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


//    public boolean handleTuple(Tuple tuple, Object key){
//        int route = _routingTable.route(key);
//        if(route < 0)
//            return false;
//        try {
//            _queues.get(route).put(tuple);
//            return true;
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            return true;
//        }
//
//    }


    public static ElasticTasks createHashRouting(int numberOfRoutes, BaseElasticBolt bolt, int taskID, ElasticOutputCollector collector) {
        RoutingTable routingTable = new HashingRouting(numberOfRoutes);
        ElasticTasks ret = new ElasticTasks(bolt, taskID);
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

    public void createAndLaunchElasticTasks() {
        for(int i : get_routingTable().getRoutes())
        {
            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
            _queues.put(i, inputQueue);

            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
            _queryRunnables.put(i, query);
            Thread newThread = new Thread(query);
            newThread.start();
            _queryThreads.put(i, newThread);
            System.out.println("created elastic worker threads for route "+i);
        }
    }

    /**
     *
     * @param list list of exception routes
     * @return a PartialHashRouting that routes the excepted routes
     */
    public synchronized PartialHashingRouting addExceptionForHashRouting(ArrayList<Integer> list, LinkedBlockingQueue<ITaskMessage> exceptedRoutingQueue) {

        if(!(_routingTable instanceof HashingRouting)) {
            System.err.println("cannot set Exception for non-hash routing");
            return null;
        }
        for(int i: list) {
            if(!_routingTable.getRoutes().contains(i)) {
                System.err.println("input route " + i + "is invalid");
                return null;
            }
        }

        _remoteTupleQueue = exceptedRoutingQueue;

        HashingRouting routing = (HashingRouting)_routingTable;
        if(!(_routingTable instanceof PartialHashingRouting)) {
            _routingTable = new PartialHashingRouting(routing);
        }
        ((PartialHashingRouting)_routingTable).addExceptionRoutes(list);

        for(int i: list) {
            terminateGivenQuery(i);
        }
        PartialHashingRouting ret = ((PartialHashingRouting)_routingTable).createComplementRouting();
        ret.invalidAllRoutes();
        ret.addValidRoutes(list);
        return ret;
    }

    public PartialHashingRouting addExceptionForHashRouting(int r, LinkedBlockingQueue<ITaskMessage> exceptedRoutingQueue) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(r);
        return addExceptionForHashRouting(list, exceptedRoutingQueue);
    }

    public synchronized void setHashRouting(int numberOfRoutes) throws IllegalArgumentException {

        if(numberOfRoutes <= 0)
            throw new IllegalArgumentException("number of routes should be positive");

        System.out.println("##########before termination!");
        if(!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
        }
        System.out.println("##########after termination!");
        _routingTable = new HashingRouting(numberOfRoutes);
        createAndLaunchElasticTasks();
        System.out.println("##########after launch tasks!");
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

    public int get_taskID() {
        return _taskID;
    }

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }


    private void terminateGivenQuery(int route) {
        _queryRunnables.get(route).terminate();
        try {
            _queryThreads.get(route).join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void terminateQueries() {
        for(QueryRunnable query: _queryRunnables.values()) {
            query.terminate();
        }
        _queryRunnables.clear();
        for(Thread thread: _queryThreads.values()) {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        }
        _queryThreads.clear();
        _queues.clear();
    }



}
