package backtype.storm.elasticity.message.taksmessage;
import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.state.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Robert on 11/16/15.
 */
public class RemoteState implements ITaskMessage {

    public int _taskId;
    public  Map<Object, Object> _state;
    public List<Integer> _routes = new ArrayList<>();
    public boolean finalized = false;


    public RemoteState(int taskid, Map<Object, Object> state, int route) {
        _taskId = taskid;
        _state = state;
        _routes.add(route);
    }

    public RemoteState(int taskid,  Map<Object, Object> state, List<Integer> routes) {
        _taskId = taskid;
        _state = state;
        _routes = routes;
    }

    public void markAsFinalized() {
        finalized = true;
    }


}
