package backtype.storm.elasticity.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 * Created by robert on 11/25/15.
 */
public class KeyFrequencySampler implements Serializable{

    private Map<Integer, Long> counts = new HashMap<>();

    double _sampleRate;

    transient Random _random = new Random();

    public KeyFrequencySampler(double sampleRate) {
        _sampleRate = sampleRate;
    }

    public synchronized void record(int key) {
        if(_random.nextDouble()<_sampleRate) {
            if(!counts.containsKey(key)) {
                counts.put(key,0L);
            }
            counts.put(key, counts.get(key)+1);
        }
    }

    public String toString() {
        String ret="";
        for(Object key: counts.keySet()) {
            ret += key+": " + counts.get(key) +"\n";
        }
        return ret;
    }

    public synchronized void clear() {
        counts.clear();
    }

    public Histograms getDistribution() {
        Histograms ret = new Histograms(counts);
        clear();
        return ret;
    }
}
