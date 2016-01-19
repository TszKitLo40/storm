package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Robert on 11/4/15.
 */
public class TupleExecuteResult implements Serializable{


    public static final int Emit = 0;
    public static final int EmitDirect = 1;

    protected int _taskId;

    protected String _streamId;

    transient protected Tuple _inputTuple;

    protected List<Object> _outputTuple;

    protected int _commandType;

    public TupleExecuteResult(int taskId, String streamId, Tuple inputTuple, List<Object> outputTuple, int command) {
        _taskId = taskId;
        _streamId = streamId;
        _inputTuple = inputTuple;
        _outputTuple = outputTuple;
        _commandType = command;
    }



    public static TupleExecuteResult createEmit(String streamId, Tuple inputTuple, List<Object> outputTuple) {
        return new TupleExecuteResult(0, streamId, inputTuple, outputTuple, Emit);
    }


}
