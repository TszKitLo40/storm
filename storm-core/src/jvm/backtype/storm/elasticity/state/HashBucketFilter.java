package backtype.storm.elasticity.state;

import backtype.storm.elasticity.utils.GlobalHashFunction;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by robert on 12/16/15.
 */
public class HashBucketFilter implements StateFilter {

    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    private HashSet<Integer> validBuckets = new HashSet<>();

    int numberOfBuckets;

    public HashBucketFilter(int numberOfBuckets, ArrayList<Integer> validBuckets) {
        this.validBuckets.addAll(validBuckets);
        this.numberOfBuckets = numberOfBuckets;
    }

    public HashBucketFilter(int numberOfBuckets, int bucket) {
        this.validBuckets.add(bucket);
        this.numberOfBuckets = numberOfBuckets;
    }

    @Override
    public Boolean isValid(Object key) {
        return validBuckets.contains(hashFunction.hash(key) % numberOfBuckets);
    }
}
