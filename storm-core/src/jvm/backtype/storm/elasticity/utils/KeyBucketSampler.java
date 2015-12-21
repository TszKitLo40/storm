package backtype.storm.elasticity.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;

/**
 * Created by robert on 11/26/15.
 */
public class KeyBucketSampler {

    private int _nBuckets;

    public Long[] buckets;

    private boolean enabled = false;


    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    public KeyBucketSampler(int nbucks) {
        _nBuckets = nbucks;
        buckets = new Long[_nBuckets];
    }

    public synchronized void record(Object key) {
        if(enabled) {
            final int bucket = hashFunction.hash(key)%_nBuckets;
            buckets[bucket]++;
        }
    }

    public synchronized void enable() {
        enabled = true;
    }

    public synchronized void disable() {
        enabled = false;
    }

    public void clear() {
        for(int i=0; i<_nBuckets; i++) {
            buckets[i] = 0L;
        }
    }

    public String toString() {
        String ret = "";
        for(long i: buckets) {
            ret += i + "\n";
        }
        return ret;
    }
}
