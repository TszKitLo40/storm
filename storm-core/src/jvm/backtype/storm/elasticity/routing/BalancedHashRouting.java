package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.GlobalHashFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by robert on 11/26/15.
 */
public class BalancedHashRouting implements RoutingTable {

    GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    int numberOfRoutes;

    Map<Integer, Integer> hashValueToRoute;

    int numberOfHashValues;

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        hashValueToRoute = new HashMap<Integer, Integer>();
        hashValueToRoute.putAll(hashValueToPartition);
        numberOfHashValues = hashValueToPartition.size();
//        for(Integer key: hashValueToPartition.keySet()) {
//            hashValueToRoute.put(key, hashValueToPartition.get(key));
//        }
    }

    @Override
    public synchronized int route(Object key) {

        return hashValueToRoute.get(hashFunction.hash(key) % numberOfHashValues);
    }

    @Override
    public int getNumberOfRoutes() {
        return numberOfRoutes;
    }

    @Override
    public ArrayList<Integer> getRoutes() {
        ArrayList<Integer> ret = new ArrayList<>();
        for(int i=0; i<numberOfRoutes; i++) {
            ret.add(i);
        }
        return ret;
    }

    public Set<Integer> getBucketSet() {
        return hashValueToRoute.keySet();
    }

    public synchronized void reassignBucketToRoute(int bucketid, int targetRoute) {
        hashValueToRoute.put(bucketid, targetRoute);
    }

    public String toString() {
        String ret = "Balanced Hash Routing: \n";
        ret += "number of routes: " + getNumberOfRoutes() +"\n";
        ret += "Bucket to route maps:\n";
        ret += hashValueToRoute;
        ret +="\n";
        return ret;
    }

    public int getNumberOfBuckets() {
        return numberOfHashValues;
    }
}
