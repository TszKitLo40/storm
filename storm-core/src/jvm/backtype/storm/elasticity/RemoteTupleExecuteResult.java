package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by Robert on 11/14/15.
 */
public class RemoteTupleExecuteResult extends TupleExecuteResult implements ITaskMessage {

    public int _originalTaskID;

    public RemoteTupleExecuteResult(int orignalTaskID, int taskId, String streamId, Tuple inputTuple, List<Object> outputTuple, int command) {
        super(taskId,streamId,inputTuple,outputTuple,command);
        _originalTaskID = orignalTaskID;
    }

    public static RemoteTupleExecuteResult createEmit(int originalTaskID, String streamId, Tuple inputTuple, List<Object> outputTuple) {
        return new RemoteTupleExecuteResult(originalTaskID,0, streamId, inputTuple, outputTuple, Emit);
    }

}
