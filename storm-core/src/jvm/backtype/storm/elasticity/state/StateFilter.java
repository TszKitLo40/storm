package backtype.storm.elasticity.state;

/**
 * Created by robert on 12/16/15.
 */
public interface StateFilter {

    public Boolean isValid(Object key);
}
