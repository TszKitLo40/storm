package backtype.storm.elasticity;

import backtype.storm.elasticity.routing.RoutingTable;

import java.io.Serializable;

/**
 * Created by Robert on 11/12/15.
 */
public class RemoteElasticBolt implements Serializable{

    RoutingTable _routingTable;

    BaseElasticBolt _bolt;

    int _taskID;

    public RemoteElasticBolt(RoutingTable routingTable, BaseElasticBolt bolt, int taskId) {
        _routingTable = routingTable;
        _bolt = bolt;
        _taskID = taskId;
    }

    public void prepare() {

    }


}
