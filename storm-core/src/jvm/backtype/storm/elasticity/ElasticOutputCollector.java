package backtype.storm.elasticity;

import backtype.storm.topology.IBasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/4/15.
 */
public class ElasticOutputCollector {

    protected LinkedBlockingQueue<TupleExecuteResult> _outputQueue;

    public ElasticOutputCollector(LinkedBlockingQueue outputQueue) {
        _outputQueue = outputQueue;
    }

    // As the emit is delayed, the destination tasks are unknown.
    public List<Integer> emit(String streamId, Tuple inputTuple, List<Object> tuple) {
        try {
            _outputQueue.put(TupleExecuteResult.createEmit(streamId, inputTuple, tuple));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }


    public List<Integer> emit(Tuple inputTuple, List<Object> tuple) {
        try {
            _outputQueue.put(TupleExecuteResult.createEmit(Utils.DEFAULT_STREAM_ID, inputTuple, tuple));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Integer> emit(List<Object> tuple) {
        try {
            _outputQueue.put(TupleExecuteResult.createEmit(Utils.DEFAULT_STREAM_ID, null, tuple));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        assert(false);
    }

    public void reportError(Throwable error) {

    }
}
