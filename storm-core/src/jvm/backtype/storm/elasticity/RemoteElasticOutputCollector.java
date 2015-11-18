package backtype.storm.elasticity;

import backtype.storm.elasticity.message.taksmessage.RemoteTupleExecuteResult;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/14/15.
 */
public class RemoteElasticOutputCollector extends ElasticOutputCollector {

    int _originalTaskId;

    public RemoteElasticOutputCollector(LinkedBlockingQueue outputQueue, int originalTaskId) {
        super(outputQueue);
        _originalTaskId = originalTaskId;
    }


    // As the emit is delayed, the destination tasks are unknown.
    @Override
    public List<Integer> emit(String streamId, Tuple inputTuple, List<Object> tuple) {
        _outputQueue.add(RemoteTupleExecuteResult.createEmit(_originalTaskId,streamId, inputTuple, tuple));
        return null;
    }

    @Override
    public List<Integer> emit(Tuple inputTuple, List<Object> tuple) {
        _outputQueue.add(RemoteTupleExecuteResult.createEmit(_originalTaskId,Utils.DEFAULT_STREAM_ID, inputTuple, tuple));
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        assert(false);
    }

    @Override
    public void reportError(Throwable error) {

    }

}
