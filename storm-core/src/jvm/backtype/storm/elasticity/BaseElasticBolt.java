package backtype.storm.elasticity;

import backtype.storm.state.KeyValueState;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Robert on 11/3/15.
 */
public abstract class BaseElasticBolt implements Serializable {

    private KeyValueState state = new KeyValueState();

    public abstract Object getKey(Tuple tuple);

    public Object getValueByKey(Object key) {
        return state.getValueByKey(key);
    }

    public void setValueByKey(Object key, Object value) {
        state.setValueBySey(key, value);
    }

    public abstract void execute(Tuple input, ElasticOutputBuffer collector);

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

    public void prepare(Map stormConf, TopologyContext context) {};

    public void cleanup() {

    }
}
