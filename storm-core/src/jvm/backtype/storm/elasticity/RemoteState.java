package backtype.storm.elasticity;
import backtype.storm.elasticity.ITaskMessage;
import backtype.storm.elasticity.state.*;

import java.util.ArrayList;

/**
 * Created by Robert on 11/16/15.
 */
public class RemoteState implements ITaskMessage {

    public int _taskId;
    public KeyValueState _state;

    public RemoteState(int taskid, KeyValueState state) {
        _taskId = taskid;
        _state = state;
    }


}
