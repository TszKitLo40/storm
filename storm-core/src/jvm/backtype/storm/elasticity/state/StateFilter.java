package backtype.storm.elasticity.state;

import java.io.Serializable;

/**
 * Created by robert on 12/16/15.
 */
public interface StateFilter extends Serializable {

    public Boolean isValid(Object key);
}
