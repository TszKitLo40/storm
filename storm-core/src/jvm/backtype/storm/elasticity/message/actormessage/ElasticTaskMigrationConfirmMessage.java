package backtype.storm.elasticity.message.actormessage;

import java.util.ArrayList;

/**
 * Created by Robert on 11/15/15.
 */
public class ElasticTaskMigrationConfirmMessage implements IMessage {

    public String _ip;
    public int _port;
    public ArrayList<Integer> _routes;
    public int _taskId;

    public ElasticTaskMigrationConfirmMessage(int taskId, String ip, int port, ArrayList<Integer> routes) {
        _taskId = taskId;
        _ip = ip;
        _port = port;
        _routes = routes;
    }


}
