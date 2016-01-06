package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.state.StateFilter;

/**
 * Created by robert on 12/16/15.
 */
public class StateFlushToken implements ITaskMessage {

    public int _taskId;
    public int _targetRoute;
    public StateFilter _filter;

    public StateFlushToken(int taskId, int targetRoute, StateFilter filter) {
        _taskId = taskId;
        _targetRoute = targetRoute;
        _filter = filter;
    }

}
