package backtype.storm.elasticity.routing;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;

/**
 * Created by Robert on 11/3/15.
 */
public class HashingRouting implements RoutingTable {

    private int numberOfRoutes;

    HashFunction hashFunction;


    /**
     *
     * @param nRoutes is the number of routes processed by elastic tasks.
     */
    public HashingRouting(int nRoutes) {
        numberOfRoutes = nRoutes;
        hashFunction = Hashing.murmur3_32();

    }

    public HashingRouting(HashingRouting hashingRouting) {
        numberOfRoutes = hashingRouting.numberOfRoutes;
        hashFunction = Hashing.murmur3_32();


    }

    /**
     * TODO: the module hash funcion may result in skewness. A better hash function is needed here.
     * @param key the key of the input tuple.
     * @return the number of route this key belongs to.
     */
    @Override
    public int route(Object key) {
//        if(key instanceof String) {
//            final int hashvalue = hashFunction.hashString(key.toString()).asInt();
//            return Math.abs(hashvalue%(numberOfRoutes + 1)) - 1;
//        } else {
            final int hashValue = key.hashCode();


            return Math.abs((hashValue*1171+5843))%9973%(numberOfRoutes);
//        }
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
