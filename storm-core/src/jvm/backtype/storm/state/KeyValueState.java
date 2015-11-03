package backtype.storm.state;

import java.io.Serializable;
import java.util.HashMap;

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
}
