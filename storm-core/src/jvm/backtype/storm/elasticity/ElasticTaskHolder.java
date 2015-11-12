package backtype.storm.elasticity;

import backtype.storm.elasticity.ActorFramework.Slave;
import backtype.storm.messaging.IContext;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    public static Logger LOG = LoggerFactory.getLogger(ElasticTaskHolder.class);

    private static ElasticTaskHolder _instance;

    private IContext _context;

    private String _stormId;

    private int _port;

    private Slave _slaveActor;

    Map<Integer, BaseElasticBoltExecutor> _bolts = new HashMap<>();

    static public ElasticTaskHolder instance() {
        return _instance;
    }

    static public ElasticTaskHolder createAndGetInstance(IContext iContext, String stormId, int port) {
        if(_instance==null) {
            _instance = new ElasticTaskHolder(iContext, stormId, port);
        }
        return _instance;
    }



    private ElasticTaskHolder(IContext iContext, String stormId, int port) {

        _context = iContext;
        _stormId = stormId;
        _port = port + 10000;
        _slaveActor = Slave.createActor(_stormId,Integer.toString(port));
        LOG.info("ElasticTaskHolder is launched.");
        LOG.info("storm id:"+stormId+" port:" + port);
    }

    public void registerElasticBolt(BaseElasticBoltExecutor bolt, int taskId) {
        _bolts.put(taskId, bolt);
        LOG.info("A new ElasticTask is registered." + taskId);
    }

    public void sendMessageToMaster(String message) {
        _slaveActor.sendMessageToMaster(message);
    }
    

}
