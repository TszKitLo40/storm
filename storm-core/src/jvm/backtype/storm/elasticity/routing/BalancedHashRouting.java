package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.SlideWindowKeyBucketSample;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by robert on 11/26/15.
 */
public class BalancedHashRouting implements RoutingTable {

    GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    int numberOfRoutes;

    Map<Integer, Integer> hashValueToRoute;

    int numberOfHashValues;

    SlideWindowKeyBucketSample sample;

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        hashValueToRoute = new HashMap<>();
        hashValueToRoute.putAll(hashValueToPartition);
        numberOfHashValues = hashValueToPartition.size();
        sample = new SlideWindowKeyBucketSample(numberOfHashValues);
        sample.enable();
    }

    @Override
    public synchronized int route(Object key) {
        sample.record(key);

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

//        ArrayList<ArrayList<Integer>> routeToBuckets = new ArrayList<ArrayList<Integer>>();
//        for(int i=0; i < numberOfRoutes; i++ ) {
//            routeToBuckets.add(new ArrayList<Integer>());// = new ArrayList<>();
//        }

        NumberFormat formatter = new DecimalFormat("#0.0000");

        ArrayList<Integer>[] routeToBuckets = new ArrayList[numberOfRoutes];
        for(int i=0; i< numberOfRoutes; i++) {
            routeToBuckets[i] = new ArrayList<>();
        }

        for(int bucket: hashValueToRoute.keySet()) {
            routeToBuckets[hashValueToRoute.get(bucket)].add(bucket);
        }

        for(ArrayList<Integer> list: routeToBuckets) {
            Collections.sort(list);
        }

        String ret = "Balanced Hash Routing: \n";
        ret += "number of routes: " + getNumberOfRoutes() +"\n";
        ret += "Route Details:\n";

        Double[] bucketFrequencies = sample.getFrequencies();

        for(int i = 0; i < routeToBuckets.length; i++) {
            double sum = 0;
            ret += "Route " + i + ": ";
            for(Integer bucket: routeToBuckets[i]) {
                sum += bucketFrequencies[bucket];
                ret += bucket + " (" + formatter.format(bucketFrequencies[bucket]) + ")  ";
            }
            ret +="total = " + formatter.format(sum) + "\n";
        }





//        ret += hashValueToRoute;
//        ret +="\n";
        return ret;
    }

    public int getNumberOfBuckets() {
        return numberOfHashValues;
    }
}
