package backtype.storm.elasticity;

import backtype.storm.elasticity.exceptions.InvalidRouteException;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.message.taksmessage.RemoteTuple;
import backtype.storm.elasticity.routing.*;
import backtype.storm.elasticity.utils.KeyFrequencySampler;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import backtype.storm.elasticity.state.*;

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

    public transient KeyFrequencySampler _sample;

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
        _sample = new KeyFrequencySampler(0.05);
    }

    public void prepare(ElasticOutputCollector elasticOutputCollector, KeyValueState state) {
        _bolt.setState(state);
        for(Object key: state.getState().keySet()) {
            System.out.println("State <"+key+", "+state.getValueByKey(key)+"> has been restored!");
        }
        _queues = new HashMap<>();
        _queryThreads = new HashMap<>();
        _queryRunnables = new HashMap<>();
        _elasticOutputCollector = elasticOutputCollector;
        _sample = new KeyFrequencySampler(0.05);
    }

    public RoutingTable get_routingTable() {
        return _routingTable;
    }

    public void set_routingTable(RoutingTable routingTable) {
        _routingTable = routingTable;
    }

    public synchronized boolean tryHandleTuple(Tuple tuple, Object key) {
        int route = _routingTable.route(key);
        _sample.record(route);
//        if(route==RoutingTable.origin)
//            return false;
//        else
        if (route == RoutingTable.remote) {
//            System.out.println("a tuple is routed to remote!");

            RemoteTuple remoteTuple = new RemoteTuple(_taskID, ((PartialHashingRouting)_routingTable).getOrignalRoute(key), tuple);
            try {
                _remoteTupleQueue.put(remoteTuple);
//                System.out.println("Remote Tuple is generated and sent to _remoteTupleQueue");
            } catch (InterruptedException e) {
                e.printStackTrace();

            }
            return true;
        }
        else {
            try {
//                System.out.println("A tuple is route to "+route+ "by the routing table!");
                _queues.get(route).put(tuple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NullPointerException ne) {
                ne.printStackTrace();
//                System.err.println("route:"+route+", queue size: "+_queues.size());
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
        ret._routingTable = routingTable;
        ret.prepare(collector);
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

    public static ElasticTasks createVoidRouting(BaseElasticBolt bolt, int taskID,ElasticOutputCollector collector ) {
        ElasticTasks ret = new ElasticTasks(bolt, taskID);
        ret.prepare(collector);
        return ret;
    }

    public void createAndLaunchElasticTasks() {
        for(int i : get_routingTable().getRoutes())
        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            _queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
//            _queryRunnables.put(i, query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            _queryThreads.put(i, newThread);
//            System.out.println("created elastic worker threads for route "+i);
//            createAndLaunchElasticTasksForGivenRoute(i);
            createElasticTasksForGivenRoute(i);
            launchElasticTasksForGivenRoute(i);
        }
        System.out.println("##########after launch tasks!");

    }

    public void createElasticTasksForGivenRoute(int i) {
        if(!_routingTable.getRoutes().contains(i)) {
            System.out.println("Cannot create tasks for route "+i+", because it is not valid!");
            return;
        }
        LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>(64);
        _queues.put(i, inputQueue);
    }

    public void launchElasticTasksForGivenRoute(int i) {
        LinkedBlockingQueue<Tuple> inputQueue = _queues.get(i);
        QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
        _queryRunnables.put(i, query);
        Thread newThread = new Thread(query);
        newThread.start();
        _queryThreads.put(i, newThread);
        System.out.println("created elastic worker threads for route "+i);
        ElasticTaskHolder.instance().sendMessageToMaster("created elastic worker threads for route "+i);
    }

    public void createAndLaunchElasticTasksForGivenRoute(int i) {
        if(!_routingTable.getRoutes().contains(i)) {
            System.out.println("Cannot create tasks for route "+i+", because it is not valid!");
            return;
        }
        LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
        _queues.put(i, inputQueue);

        QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
        _queryRunnables.put(i, query);
        Thread newThread = new Thread(query);
        newThread.start();
        _queryThreads.put(i, newThread);
        System.out.println("created elastic worker threads for route "+i);
    }

    /**
     *
     * @param list list of exception routes
     * @return a PartialHashRouting that routes the excepted routes
     */
    public synchronized PartialHashingRouting addExceptionForHashRouting(ArrayList<Integer> list, LinkedBlockingQueue<ITaskMessage> exceptedRoutingQueue) throws InvalidRouteException, RoutingTypeNotSupportedException {

        if(!(_routingTable instanceof PartialHashingRouting)&&!(_routingTable instanceof BalancedHashRouting)) {
            throw new RoutingTypeNotSupportedException("cannot set Exception for non-hash routing: " + _routingTable);
//            System.err.println("cannot set Exception for non-hash routing");
//            return null;
        }
        for(int i: list) {
            if(!_routingTable.getRoutes().contains(i)) {
                throw new InvalidRouteException("input route " + i + "is invalid");
//                System.err.println("input route " + i + "is invalid");
//                return null;
            }
        }

        _remoteTupleQueue = exceptedRoutingQueue;

//        HashingRouting routing = (HashingRouting)_routingTable;
        if(!(_routingTable instanceof PartialHashingRouting)) {
            _routingTable = new PartialHashingRouting(_routingTable);
        }

        System.out.println("Original Routing (Before adding exception): getRoutes:" +get_routingTable().getRoutes());
        ((PartialHashingRouting)_routingTable).addExceptionRoutes(list);

        for(int i: list) {
            System.out.println("Terminating the thread for route " + i);
            terminateGivenQuery(i);
        }
        System.out.println("Original Routing (After adding exception): getRoutes:" +get_routingTable().getRoutes());
        PartialHashingRouting ret = ((PartialHashingRouting)_routingTable).createComplementRouting();
        System.out.println("Complement Routing: getRoutes:" + ret.getRoutes());
        ret.invalidAllRoutes();
        ret.addValidRoutes(list);
        System.out.println("Complement Routing: getRoutes:" +ret.getRoutes());

        return ret;
    }

    public PartialHashingRouting addExceptionForHashRouting(int r, LinkedBlockingQueue<ITaskMessage> exceptedRoutingQueue) throws InvalidRouteException, RoutingTypeNotSupportedException {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(r);
        return addExceptionForHashRouting(list, exceptedRoutingQueue);
    }

    /**
     * add a valid route to the routing table, but does not create the processing thread.
     * In fact, the processing thread can only be created when the remote state is merged with
     * local state, which should be handled by the callee.
     * @param route route to be added
     */
    public synchronized void addValidRoute(int route) throws RoutingTypeNotSupportedException {
        if(!(_routingTable instanceof PartialHashingRouting)) {
            System.err.println("can only add valid route for PartialHashRouting");
            throw new RoutingTypeNotSupportedException("can only add valid route for PartialHashRouting!");
        }

        PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;

        partialHashingRouting.addValidRoute(route);

        createElasticTasksForGivenRoute(route);

    }

    public synchronized void setHashBalancedRouting(int numberOfRoutes, Map<Integer, Integer> hashValueToPartition) {
        if (numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive!");
        withDrawCurrentRouting();
        _routingTable = new BalancedHashRouting(hashValueToPartition, numberOfRoutes);

        createAndLaunchElasticTasks();


    }

    public synchronized void setHashRouting(int numberOfRoutes) throws IllegalArgumentException {

        if(numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive");

//        System.out.println("##########before termination!");
//        if((_routingTable instanceof HashingRouting)) {
//
//            terminateQueries();
//            if(_routingTable instanceof PartialHashingRouting) {
//                PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;
//                for(int i: partialHashingRouting.getExceptionRoutes()) {
//                    try {
//                        ElasticTaskHolder.instance().withdrawRemoteElasticTasks(get_taskID(), i);
//                    } catch(Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//        System.out.println("##########after termination!");
//        clearDistributionSample();

        withDrawCurrentRouting();


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

    public int get_taskID() {
        return _taskID;
    }

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }


    public void terminateGivenQuery(int route) {
        ElasticTaskHolder.instance().sendMessageToMaster("Terminating "+_taskID+"."+route + " (" + _queues.get(route).size() + " pending elements)"+" ...");
        _queryRunnables.get(route).terminate();
        try {
            _queryThreads.get(route).join();
            System.out.println("Query thread for "+_taskID+"."+route + " is terminated!");
            ElasticTaskHolder.instance().sendMessageToMaster("Query thread for "+_taskID+"."+route + " is terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        _queryRunnables.remove(route);
        _queryThreads.remove(route);
        _queues.remove(route);

    }

    private void withDrawCurrentRouting() {
        System.out.println("##########before termination!");
//        if((_routingTable instanceof RoutingTable)) {

            terminateQueries();
            if(_routingTable instanceof PartialHashingRouting) {
                PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;
                for(int i: partialHashingRouting.getExceptionRoutes()) {
                    try {
                        ElasticTaskHolder.instance().withdrawRemoteElasticTasks(get_taskID(), i);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
//        }
        System.out.println("##########after termination!");
        clearDistributionSample();
    }

    private void terminateQueries() {

        for(int i: _routingTable.getRoutes())
            terminateGivenQuery(i);
//
//        for(QueryRunnable query: _queryRunnables.values()) {
//            query.terminate();
//        }
//        _queryRunnables.clear();
//        for(Thread thread: _queryThreads.values()) {
//            try {
//                thread.join();
//            } catch (InterruptedException e) {
//
//            }
//        }
//        _queryThreads.clear();
//        _queues.clear();
    }
//
//    public void terminateAGivenQuery(int route) throws InvalidRouteException {
//        if(!_queryThreads.containsKey(route)&&!_queryRunnables.containsKey(route)||!_queues.containsKey(route)) {
//            throw new InvalidRouteException("ElasticTasks does not have route " + route + ".");
//        }
//        _queryRunnables.get(route).terminate();
//        try {
//            _queryThreads.get(route).join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        _queryRunnables.remove(route);
//        _queryThreads.remove(route);
//        _queues.remove(route);
//    }

    private void clearDistributionSample() {
        if(_sample!=null) {
            _sample.clear();
        }
    }


}
