package backtype.storm.elasticity.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 11/3/15.
 */
public class KeyValueState implements Serializable {

    HashMap<Object, Object> state = new HashMap<>();

    public Object getValueByKey(Object key) {
        if (state.containsKey(key))
            return state.get(key);
        else
            return null;
    }

    public void setValueBySey(Object key, Object value) {
        state.put(key,value);
    }

    public void update(KeyValueState newState) {
        state.putAll(newState.state);
    }

    public HashMap<Object, Object> getState() {
        return state;
    }

    public KeyValueState getValidState(StateFilter filter) {
        KeyValueState ret = new KeyValueState();
        for(Object key: state.keySet()) {
            if(filter.isValid(key)) {
                ret.setValueBySey(key, state.get(key));
            }
        }
        return ret;
    }
}
