package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.TupleExecuteResult;
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

}
