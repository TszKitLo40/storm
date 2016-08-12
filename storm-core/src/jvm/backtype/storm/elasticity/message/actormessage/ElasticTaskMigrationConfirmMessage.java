package backtype.storm.elasticity.message.actormessage;

import java.util.List;

/**
 * Created by Robert on 11/15/15.
 */
public class ElasticTaskMigrationConfirmMessage implements IMessage {

    public String _ip;
    public int _port;
    public List<Integer> _routes;
    public int _taskId;

    public ElasticTaskMigrationConfirmMessage(int taskId, String ip, int port, List<Integer> routes) {
        _taskId = taskId;
        _ip = ip;
        _port = port;
        _routes = routes;
    }


}
