package backtype.storm.elasticity.routing;

import java.util.ArrayList;

/**
 * Created by Robert on 11/3/15.
 */
public class HashingRouting implements RoutingTable {

    private int numberOfRoutes;

    /**
     *
     * @param nRoutes is the number of routes processed by elastic tasks.
     */
    public HashingRouting(int nRoutes) {
        numberOfRoutes = nRoutes;
    }

    public HashingRouting(HashingRouting hashingRouting) {
        numberOfRoutes = hashingRouting.numberOfRoutes;
    }

    /**
     * TODO: the module hash funcion may result in skewness. A better hash function is needed here.
     * @param key the key of the input tuple.
     * @return the number of route this key belongs to.
     */
    @Override
    public int route(Object key) {
        return Math.abs(key.hashCode())%(numberOfRoutes + 1) - 1;
    }

    @Override
    public int getNumberOfRoutes() {
        return numberOfRoutes;
    }

    @Override
    public ArrayList<Integer> getRoutes() {
        ArrayList<Integer> ret = new ArrayList<>();
        for(int i=0;i<numberOfRoutes;i++) {
            ret.add(i);
        }
        return ret;
    }
}
