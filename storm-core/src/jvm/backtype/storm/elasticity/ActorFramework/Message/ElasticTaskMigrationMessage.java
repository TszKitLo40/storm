package backtype.storm.elasticity.ActorFramework.Message;

import backtype.storm.elasticity.ElasticTasks;

/**
 * Created by Robert on 11/12/15.
 */
public class ElasticTaskMigrationMessage implements IMessage {

    public ElasticTasks _elasticTask;

    public int _port;

    public String _ip;

    public ElasticTaskMigrationMessage(ElasticTasks task, int port) {
        _elasticTask = task;
//        _ip = ip;
        _port = port;
    }

    public String getString() {
        return "source: "+_ip+":"+_port+" task id: "+_elasticTask.get_taskID();
    }
}
