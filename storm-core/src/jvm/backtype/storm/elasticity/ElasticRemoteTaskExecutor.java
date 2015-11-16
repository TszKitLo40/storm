package backtype.storm.elasticity;

import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.tuple.Tuple;
import backtype.storm.elasticity.state.*;

import java.util.ArrayList;
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

    Runnable _stateCheckpointRunnable;

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
        _stateCheckpointRunnable = new StateCheckoutPointing(10);
        _stateCheckpointingThread = new Thread(_stateCheckpointRunnable);
        _stateCheckpointingThread.start();

        System.out.println("state checkpointing thread is created");
    }

    class InputTupleRouting implements Runnable {

        boolean _terminated = false;

        @Override
        public void run() {
            while(!_terminated) {
                try {

                    Tuple input = _inputQueue.take();

                    boolean handled = _elasticTasks.tryHandleTuple(input, _bolt.getKey(input));

                    System.out.println("@"+hashCode()+"A remote tuple for " + _elasticTasks.get_taskID()+"."+_elasticTasks.get_routingTable().route(_bolt.getKey(input))+"has been processed");

                    assert(handled);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
            while(!_terminated) {
                try {
                    Thread.sleep(1000*_cycleInSecs);
                    KeyValueState state = _elasticTasks.get_bolt().getState();
                    for (Object key : state.getState().keySet()) {
                        if (_elasticTasks.get_routingTable().route(key) < 0) {
                            state.getState().remove(key);
                            System.out.println("Key " + key + "will be removed before migration, because it belongs to an invalid route");
                        }
                    }
                    RemoteState remoteState = new RemoteState(_elasticTasks.get_taskID(), state);
                    System.out.println("State ("+state.getState().size()+" element) has been added into the sending queue!");
                    _resultQueue.put(remoteState);
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }
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

}
