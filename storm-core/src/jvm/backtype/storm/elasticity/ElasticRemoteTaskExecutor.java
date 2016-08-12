package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.message.taksmessage.CleanPendingTupleToken;
import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.message.taksmessage.PendingTupleCleanedMessage;
import backtype.storm.elasticity.message.taksmessage.RemoteState;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.tuple.Tuple;
import backtype.storm.elasticity.state.*;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.SerializationUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Robert on 11/14/15.
 */
public class ElasticRemoteTaskExecutor {

    ElasticTasks _elasticTasks;

    LinkedBlockingQueue<ITaskMessage> _resultQueue;

    LinkedBlockingQueue<Object> _inputQueue = new LinkedBlockingQueue<>(Config.RemoteExecutorInputQueueCapacity);

    RemoteElasticOutputCollector _outputCollector;

    BaseElasticBolt _bolt;

    Thread _processingThread;

    InputTupleRouting _processingRunnable;

    Thread _stateCheckpointingThread;
    StateCheckoutPointing _checkPointing;


//    Runnable _stateCheckpointRunnable;

    public ElasticRemoteTaskExecutor(ElasticTasks tasks, LinkedBlockingQueue resultQueue, BaseElasticBolt bolt ) {
        _elasticTasks = tasks;
        _resultQueue = resultQueue;
        _bolt = bolt;
    }

    public void prepare(Map<Object, Object> state ) {
        _outputCollector = new RemoteElasticOutputCollector(_resultQueue, _elasticTasks.get_taskID());
        KeyValueState keyValueState = new KeyValueState();
        keyValueState.getState().putAll(state);
        _elasticTasks.prepare(_outputCollector, keyValueState);
        _elasticTasks.createAndLaunchElasticTasks();
        createProcessingThread();
        createStateCheckpointingThread();
    }

    public void createProcessingThread() {
        _processingRunnable = new InputTupleRouting();
        _processingThread = new Thread(_processingRunnable);
        _processingThread.start();
//        ElasticTaskHolder.instance().createQueueUtilizationMonitoringThread(_inputQueue, "Remote Input Queue", Config.RemoteExecutorInputQueueCapacity, 0.9, 0.1);

        System.out.println("processing thread is created!");
    }

    public void createStateCheckpointingThread() {
        _checkPointing = new StateCheckoutPointing(Config.StateCheckPointingCycleInSecs);
        _stateCheckpointingThread = new Thread(_checkPointing);
        _stateCheckpointingThread.start();

        System.out.println("state checkpointing thread is created");
    }

    class InputTupleRouting implements Runnable {

        boolean _terminated = false;
        boolean _terminating = false;

        public void terminate() {
            while(!_terminated) {
                Utils.sleep(1);
            }
        }

        @Override
        public void run() {
            int count = 0;
            try {
                while (!_terminating) {
                    try {
                        //                    System.out.println("poll...");
                        Object item = _inputQueue.poll(5, TimeUnit.MILLISECONDS);
                        //                    System.out.println("polled!");

                        if(item == null)
                           continue;

                        if(item instanceof Tuple) {
                            final Tuple input = (Tuple) item;
                            boolean handled = _elasticTasks.tryHandleTuple(input, _bolt.getKey(input));
                            count++;
                            if (count % 10000 == 0) {
                                System.out.println("A remote tuple for " + _elasticTasks.get_taskID() + "." + _elasticTasks.get_routingTable().route(_bolt.getKey(input)) + "has been processed");
                                count = 0;
                            }

                            if (!handled)
                                System.err.println("Failed to handle a remote tuple. There is possibly something wrong with the routing table!");

                        } else if(item instanceof CleanPendingTupleToken) {
                            final CleanPendingTupleToken token = (CleanPendingTupleToken) item;
                            //The protocol guarantees that no tuples will be sent to the target route. So a new thread can be created to do the clean job, avoiding
                            // interfering the process of other route.
                            new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    System.out.println("to handle handleCleanPendingTupleToken");
                                    _elasticTasks.makesSureNoPendingTuples(token.routeId);
                                    PendingTupleCleanedMessage message = new PendingTupleCleanedMessage(token.taskId, token.routeId);
                                    System.out.println(String.format("Pending tuple for %s.%s is cleaned!", token.taskId, token.routeId));
                                    ElasticTaskHolder.instance()._originalTaskIdToPriorityConnection.get(token.taskId).send(token.taskId, SerializationUtils.serialize(message));

                                }
                            }).start();
                        } else {
                            System.err.println("Received Unexpected Object: " + item);
                        }
                    } catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            System.out.println("InputTupleRouting thread is interrupted!");
                            throw e;
                        }
                        System.out.println(String.format("_elasticTasks: %s, _bolt: %s", _elasticTasks, _bolt));
                        e.printStackTrace();
                    }

                }

                } catch (InterruptedException  e) {
                e.printStackTrace();

            }
            _terminated = true;

            System.out.println("Routing process is terminated!");
        }
    }

    class StateCheckoutPointing implements Runnable {

        boolean _terminating = false;
        boolean _terminated = false;
        int _cycleInSecs;

        public StateCheckoutPointing(int cycleInSecs) {
            _cycleInSecs = cycleInSecs;
        }

        public StateCheckoutPointing() {
            this(10);
        }

        public void terminate() {
            _terminating = true;
            while(!_terminated) {
                Utils.sleep(1);
            }
        }

        @Override
        public void run() {

                while (!_terminating) {
                    try {
                        Thread.sleep(1000 * _cycleInSecs);
    //                    KeyValueState state = _elasticTasks.get_bolt().getState();
    //                    for (Object key : state.getState().keySet()) {
    //                        if (_elasticTasks.get_routingTable().route(key) < 0) {
    //                            state.getState().remove(key);
    //                            System.out.println("Key " + key + "will be removed before migration, because it belongs to an invalid route");
    //                        }
    //                    }
    //                    RemoteState remoteState = new RemoteState(_elasticTasks.get_taskID(), state, _elasticTasks.get_routingTable().getRoutes() );
                        RemoteState state = getStateForRoutes(_elasticTasks.get_routingTable().getRoutes());
                        System.out.println("State (" + state._state.size() + " element) has been added into the sending queue!");
                        _resultQueue.put(state);

                    } catch (InterruptedException e) {
                        System.out.println("StateCheckoutPointing is interrupted!");
                    }
                }
                _terminated = true;

        }
    }


    public RemoteState getStateForRoutes(List<Integer> routes) throws InterruptedException {
        KeyValueState state = _elasticTasks.get_bolt().getState();
        KeyValueState stateForRoutes = new KeyValueState();
        HashSet<Integer> routeSet = new HashSet<>(routes);
        for (Object key: state.getState().keySet()) {
            if(routeSet.contains(_elasticTasks.get_routingTable().route(key))) {
                stateForRoutes.setValueByKey(key, state.getValueByKey(key));
            }
        }
        RemoteState remoteState = new RemoteState(_elasticTasks.get_taskID(),stateForRoutes.getState(),routes);
//        _resultQueue.put(remoteState);
        return remoteState;
    }

    public RemoteState getStateForRoutes(int i) throws InterruptedException {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(i);
        return getStateForRoutes(routes);
    }

    public LinkedBlockingQueue<Object> get_inputQueue() {
        return _inputQueue;
    }

    public void mergeRoutingTableAndCreateCreateWorkerThreads(RoutingTable routingTable) {

        if(!(routingTable instanceof PartialHashingRouting) || !(_elasticTasks.get_routingTable() instanceof PartialHashingRouting)) {
            System.out.println("Routing table cannot be merged, when either of the routing table is not an instance of PartialHashingRouting");
            return;
        }

        if(RoutingTableUtils.getBalancecHashRouting(routingTable)!=null && RoutingTableUtils.getBalancecHashRouting(_elasticTasks.get_routingTable())!=null) {
            BalancedHashRouting exitingRouting = RoutingTableUtils.getBalancecHashRouting(_elasticTasks.get_routingTable());
            BalancedHashRouting incomingRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
            exitingRouting.update(incomingRouting);
            Slave.getInstance().sendMessageToMaster("Balanced Hash routing is updated!");
        } else {
            Slave.getInstance().sendMessageToMaster("Routing table cannot be updated as balanced routing table cannot be extracted!");
        }

        List<Integer> newRoutes = routingTable.getRoutes();
        ((PartialHashingRouting)_elasticTasks.get_routingTable()).addValidRoutes(newRoutes);
        Slave.getInstance().sendMessageToMaster("Partial Hash routing is updated!");

        Slave.getInstance().sendMessageToMaster("existing routing table: " + _elasticTasks.get_routingTable().toString());


        for(int i:routingTable.getRoutes()) {
            _elasticTasks.createAndLaunchElasticTasksForGivenRoute(i);
        }
        System.out.println("routing table is merged and the worker threads are created!");
    }

    /** There are two ways to terminate a running thread:
     *  (1) employ a termination flag to notify the thread upon termination request. The thread
     *  terminates at desirable points of execution once the flag is detected.
     *  (2) employ Thread.terminate() to terminate the thread at cancellation points (e.g., sleep,
     *  blocking system calls).
     *
     *  Method (2) is easier to implement and typically terminates as early as possible, but the
     *  termination points are out of control and hence may introduce inconsistency to the programming logic.
     *
     *  We use method (2) because termination of any cancellation points is acceptable in our logic.
     */
    public void close() {


//        _checkPointing.terminate();
//        _processingRunnable.terminate();

        _stateCheckpointingThread.interrupt();
        _processingThread.interrupt();
        try {
            _stateCheckpointingThread.join();
            _processingThread.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
