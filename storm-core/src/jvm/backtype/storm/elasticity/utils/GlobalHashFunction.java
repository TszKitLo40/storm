package backtype.storm.elasticity.utils;

/**
 * Created by robert on 11/26/15.
 */
public class GlobalHashFunction {


    private static GlobalHashFunction instance;

    public static GlobalHashFunction getInstance() {
        if(instance==null) {
            instance = new GlobalHashFunction();
        }
        return instance;
    }

    public int hash(Object key) {
        return Math.abs(key.hashCode());
    }
}
