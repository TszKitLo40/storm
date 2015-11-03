package backtype.storm.topology.base;

import backtype.storm.state.KeyValueState;
import backtype.storm.tuple.Tuple;

/**
 * Created by Robert on 11/3/15.
 */
public abstract class BaseElasticBolt extends BaseBasicBolt {

    private KeyValueState state = new KeyValueState();

    public abstract Object getKey(Tuple tuple);

    public Object getValueByKey(Object key) {
        return state.getValueByKey(key);
    }

    public void setValueByKey(Object key, Object value) {
        state.setValueBySey(key, value);
    }
}
