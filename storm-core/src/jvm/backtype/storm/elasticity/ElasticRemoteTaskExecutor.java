package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/14/15.
 */
public class ElasticRemoteTaskExecutor {

    ElasticTasks _elasticTasks;

    LinkedBlockingQueue _resultQueue;

    LinkedBlockingQueue<Tuple> _inputQueue = new LinkedBlockingQueue<>(1024*1024);

    RemoteElasticOutputCollector _outputCollector;

    BaseElasticBolt _bolt;

    Thread _processingThread;

    Runnable _processingRunnable;

    public ElasticRemoteTaskExecutor(ElasticTasks tasks, LinkedBlockingQueue resultQueue, BaseElasticBolt bolt ) {
        _elasticTasks = tasks;
        _resultQueue = resultQueue;
        _bolt = bolt;
    }

    public void prepare() {
        _outputCollector = new RemoteElasticOutputCollector(_resultQueue, _elasticTasks.get_taskID());
        _elasticTasks.prepare(_outputCollector);
        _elasticTasks.createAndLaunchElasticTasks();
    }

    public void createProcessingThread() {
        _processingRunnable = new InputTupleRouting();
        _processingThread = new Thread(_processingRunnable);
        _processingThread.start();

        System.out.println("processing thread is created!");

    }

    class InputTupleRouting implements Runnable {

        boolean _terminated = false;

        @Override
        public void run() {
            while(!_terminated) {
                try {

                    Tuple input = _inputQueue.take();

                    boolean handled = _elasticTasks.tryHandleTuple(input, _bolt.getKey(input));

                    assert(handled);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public LinkedBlockingQueue<Tuple> get_inputQueue() {
        return _inputQueue;
    }

}
