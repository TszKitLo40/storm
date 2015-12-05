package backtype.storm.elasticity;

import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.message.taksmessage.RemoteState;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.tuple.Tuple;
import backtype.storm.elasticity.state.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/14/15.
 */
public class ElasticRemoteTaskExecutor {

    ElasticTasks _elasticTasks;

    LinkedBlockingQueue<ITaskMessage> _resultQueue;

    LinkedBlockingQueue<Tuple> _inputQueue = new LinkedBlockingQueue<>(1024*1024);

    RemoteElasticOutputCollector _outputCollector;

    BaseElasticBolt _bolt;

    Thread _processingThread;

    Runnable _processingRunnable;

    Thread _stateCheckpointingThread;

//    Runnable _stateCheckpointRunnable;

    public ElasticRemoteTaskExecutor(ElasticTasks tasks, LinkedBlockingQueue resultQueue, BaseElasticBolt bolt ) {
        _elasticTasks = tasks;
        _resultQueue = resultQueue;
        _bolt = bolt;
    }

    public void prepare(KeyValueState state ) {
        _outputCollector = new RemoteElasticOutputCollector(_resultQueue, _elasticTasks.get_taskID());
        _elasticTasks.prepare(_outputCollector, state);
        _elasticTasks.createAndLaunchElasticTasks();
        createProcessingThread();
        createStateCheckpointingThread();
    }

    public void createProcessingThread() {
        _processingRunnable = new InputTupleRouting();
        _processingThread = new Thread(_processingRunnable);
        _processingThread.start();

        System.out.println("processing thread is created!");
    }

    public void createStateCheckpointingThread() {
//        _stateCheckpointRunnable = new StateCheckoutPointing(10);
        _stateCheckpointingThread = new Thread(new StateCheckoutPointing(10));
        _stateCheckpointingThread.start();

        System.out.println("state checkpointing thread is created");
    }

    class InputTupleRouting implements Runnable {

        boolean _terminated = false;

        @Override
        public void run() {
            try {
                while (!_terminated) {


                    Tuple input = _inputQueue.take();

                    boolean handled = _elasticTasks.tryHandleTuple(input, _bolt.getKey(input));

//                    System.out.println("@" + hashCode() + "A remote tuple for " + _elasticTasks.get_taskID() + "." + _elasticTasks.get_routingTable().route(_bolt.getKey(input)) + "has been processed");

                    assert (handled);


                }
            } catch (InterruptedException e) {
                System.out.println("InputTupleRouting thread is interrupted!");
            }
        }
    }

    class StateCheckoutPointing implements Runnable {

        boolean _terminated = false;
        int _cycleInSecs;

        public StateCheckoutPointing(int cycleInSecs) {
            _cycleInSecs = cycleInSecs;
        }

        public StateCheckoutPointing() {
            this(10);
        }
        @Override
        public void run() {

            try {
                while (!_terminated) {
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
                    System.out.println("State (" + state._state.getState().size() + " element) has been added into the sending queue!");
                    _resultQueue.put(state);

                }
            } catch (InterruptedException e) {
                System.out.println("StateCheckoutPointing is interrupted!");
            }
        }
    }


    public RemoteState getStateForRoutes(ArrayList<Integer> routes) throws InterruptedException {
        KeyValueState state = _elasticTasks.get_bolt().getState();
        KeyValueState stateForRoutes = new KeyValueState();
        HashSet<Integer> routeSet = new HashSet<>(routes);
        for (Object key: state.getState().keySet()) {
            if(routeSet.contains(_elasticTasks.get_routingTable().route(key))) {
                stateForRoutes.setValueBySey(key, state.getValueByKey(key));
            }
        }
        RemoteState remoteState = new RemoteState(_elasticTasks.get_taskID(),stateForRoutes,routes);
//        _resultQueue.put(remoteState);
        return remoteState;
    }

    public RemoteState getStateForRoutes(int i) throws InterruptedException {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(i);
        return getStateForRoutes(routes);
    }

    public LinkedBlockingQueue<Tuple> get_inputQueue() {
        return _inputQueue;
    }

    public void mergeRoutingTableAndCreateCreateWorkerThreads(RoutingTable routingTable) {

        if(!(routingTable instanceof PartialHashingRouting) || !(_elasticTasks.get_routingTable() instanceof PartialHashingRouting)) {
            System.out.println("Routing table cannot be merged, when either of the routing table is not an instance of PartialHashingRouting");
            return;
        }
        ArrayList<Integer> newRoutes = routingTable.getRoutes();
        ((PartialHashingRouting)_elasticTasks.get_routingTable()).addValidRoutes(newRoutes);
        for(int i:routingTable.getRoutes()) {
            _elasticTasks.createAndLaunchElasticTasksForGivenRoute(i);
        }
        System.out.println("routing table is merged and the worker threads are created!");
    }

    public void close() {
        _stateCheckpointingThread.interrupt();
//        _stateCheckpointingThread.join();
        _processingThread.interrupt();

    }

}
