package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.tuple.Tuple;


/**
 * Created by Robert on 11/15/15.
 */
public class RemoteTuple implements ITaskMessage {

    public int _taskId;
    public int _route;
    public Tuple _tuple;

    public RemoteTuple(int taskid, int route, Tuple tuple) {
        _taskId = taskid;
        _route = route;
        _tuple = tuple;
    }

    public String taskIdAndRoutePair() {
        return _taskId+"."+_route;
    }

}
