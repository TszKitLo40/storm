package backtype.storm.elasticity.state;

import backtype.storm.elasticity.utils.GlobalHashFunction;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by robert on 12/16/15.
 */
public class HashBuckerFilter implements StateFilter {

    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    private HashSet<Integer> validBuckets = new HashSet<>();

    public HashBuckerFilter(ArrayList<Integer> validBuckets) {
        this.validBuckets.addAll(validBuckets);
    }

    public HashBuckerFilter(int bucket) {
        this.validBuckets.add(bucket);
    }

    @Override
    public Boolean isValid(Object key) {
        return validBuckets.contains(hashFunction.hash(key));
    }
}
