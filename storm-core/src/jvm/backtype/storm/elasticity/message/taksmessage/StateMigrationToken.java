package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.state.StateFilter;

/**
 * Created by robert on 12/16/15.
 */
public class StateMigrationToken implements ITaskMessage {

    public int _taskId;
    public int _targetRoute;
    public StateFilter _filter;

    public StateMigrationToken(int taskId, int targetRoute, StateFilter filter) {
        _taskId = taskId;
        _targetRoute = targetRoute;
        _filter = filter;
    }

}
