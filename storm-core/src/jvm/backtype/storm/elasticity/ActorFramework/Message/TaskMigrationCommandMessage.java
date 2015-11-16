package backtype.storm.elasticity.ActorFramework.Message;

import backtype.storm.elasticity.ElasticTasks;

/**
 * Created by Robert on 11/12/15.
 */
public class TaskMigrationCommandMessage implements IMessage {

    public String _originalHostName;
    public String _targetHostName;
    public int _taskID;
    public int _route;

    public TaskMigrationCommandMessage(String originalHostName, String targetHostName, int taskId, int route) {
        _originalHostName = originalHostName;
        _targetHostName = targetHostName;
        _taskID = taskId;
        _route = route;
    }
}
