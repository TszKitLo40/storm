package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.ElasticTaskHolder;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.SlideWindowKeyBucketSample;
import backtype.storm.elasticity.utils.SlidingWindowRouteSampler;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by robert on 11/26/15.
 */
public class BalancedHashRouting implements RoutingTable, ScalableRouting {

    GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    int numberOfRoutes;

    Map<Integer, Integer> hashValueToRoute;

    int numberOfHashValues;

    transient SlideWindowKeyBucketSample sample;
    transient SlidingWindowRouteSampler routeDistributionSampler;

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        hashValueToRoute = new HashMap<>();
        hashValueToRoute.putAll(hashValueToPartition);
        numberOfHashValues = hashValueToPartition.size();
    }

    public BalancedHashRouting(int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        numberOfHashValues = Config.NumberOfShard;
        hashValueToRoute = new HashMap<>();
        for(int i = 0; i < numberOfHashValues; i++) {
            hashValueToRoute.put(i, i % numberOfRoutes);
        }
    }

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes, boolean enableSample) {
        this(hashValueToPartition, numberOfRoutes);
        if(enableSample)
            enableSampling();
    }

    public void enableSampling() {
        sample = new SlideWindowKeyBucketSample(numberOfHashValues);
        sample.enable();
    }

    @Override
    public synchronized int route(Object key) {
        if(sample!=null)
            sample.record(key);

        final int ret = hashValueToRoute.get(hashFunction.hash(key) % numberOfHashValues);
        if(routeDistributionSampler != null)
            routeDistributionSampler.record(ret);

        return ret;
    }

    @Override
    public synchronized int getNumberOfRoutes() {
        return numberOfRoutes;
    }

    @Override
    public synchronized ArrayList<Integer> getRoutes() {
        ArrayList<Integer> ret = new ArrayList<>();
        for(int i=0; i<numberOfRoutes; i++) {
            ret.add(i);
        }
        return ret;
    }

    @Override
    public Histograms getRoutingDistribution() {
        return routeDistributionSampler.getDistribution();
    }

    @Override
    public synchronized void enableRoutingDistributionSampling() {
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();
    }

    public Set<Integer> getBucketSet() {
        return hashValueToRoute.keySet();
    }

    public synchronized void reassignBucketToRoute(int bucketid, int targetRoute) {
        hashValueToRoute.put(bucketid, targetRoute);
//        ElasticTaskHolder.instance()._slaveActor.sendMessageToMaster(bucketid + ", " + targetRoute + " is put!");
    }

    public synchronized String toString() {

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

        if(sample != null) {
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
        } else {
            for(int i = 0; i < routeToBuckets.length; i++) {
                ret += "Route " + i + ": ";
                for(Integer bucket: routeToBuckets[i]) {
                    ret += bucket + "  ";
                }
                ret += "\n";
            }
        }




//        ret += hashValueToRoute;
//        ret +="\n";
        return ret;
    }

    public int getNumberOfBuckets() {
        return numberOfHashValues;
    }

    public Histograms getBucketsDistribution() {
        Histograms ret = sample.getDistribution();
        ret.setDefaultValueForAbsentKey(numberOfHashValues);

        return ret;
    }

    public Map<Integer, Integer> getBucketToRouteMapping() {
        return this.hashValueToRoute;
    }

    /**
     * create a new route (empty route)
     * @return new route id
     */
    @Override
    public synchronized int scalingOut() {
        numberOfRoutes++;
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();
        return numberOfRoutes - 1;
    }

    @Override
    public synchronized void scalingIn() {
        int largestSubtaskIndex = numberOfRoutes - 1;
        for(int shard: hashValueToRoute.keySet()) {
            if(hashValueToRoute.get(shard) == largestSubtaskIndex)
                throw new RuntimeException("There is at least one shard ("+ shard +") assigned to the Subtask with the largest index ("+ largestSubtaskIndex + "). Scaling in fails!");
        }
        numberOfRoutes--;
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();

    }
}
