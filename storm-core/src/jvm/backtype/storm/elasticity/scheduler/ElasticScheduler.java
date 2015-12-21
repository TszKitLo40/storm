package backtype.storm.elasticity.scheduler;

import backtype.storm.elasticity.actors.Master;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.utils.FirstFitDoubleDecreasing;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.generated.TaskNotExistException;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Created by Robert on 11/11/15.
 */
public class ElasticScheduler {

    Master master;

    static private ElasticScheduler instance;

    public ElasticScheduler() {
        master = Master.createActor();
        instance = this;
    }

    static public ElasticScheduler getInstance() {
        return instance;
    }

    public String optimizeBucketToRoutingMapping(int taskId) throws TaskNotExistException, RoutingTypeNotSupportedException, TException {
        // 1. get routingTable

        RoutingTable routingTable = master.getRoutingTable(taskId);
        BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
        if(balancedHashRouting == null) {
            throw new RoutingTypeNotSupportedException("Only support BalancedHashRouting family routing table!");
        }
        System.out.println("routing Table: " + balancedHashRouting.toString());



        // 2. get Distribution;

        Histograms histograms = master.getBucketDistribution(taskId);

        System.out.println("Histograms: " + histograms.toString());

        int numberOfRoutes = balancedHashRouting.getNumberOfRoutes();
        FirstFitDoubleDecreasing binPackingSolver = new FirstFitDoubleDecreasing(histograms.histogramsToArrayList(), numberOfRoutes);
        if(binPackingSolver.getResult() != numberOfRoutes) {
            System.out.println("Fail to solve the bin packing problem!");
            return null;
        }
        System.out.println(binPackingSolver.toString());


        Map<Integer, Integer> oldMapping = balancedHashRouting.getBucketToRouteMapping();
        Map<Integer, Integer> newMapping = binPackingSolver.getBucketToPartitionMap();
        ShardReassignmentPlan plan = new ShardReassignmentPlan();

        for(Integer bucket: oldMapping.keySet()) {
            if(!oldMapping.get(bucket).equals(newMapping.get(bucket)) ) {
                int oldRoute = oldMapping.get(bucket);
                int newRoute = newMapping.get(bucket);
                plan.addReassignment(taskId, bucket, oldRoute, newRoute);
                System.out.println("Move " + bucket + " from " + oldRoute + " to " + newRoute + "\n");
            }
        }

        if(!plan.getReassignmentList().isEmpty()) {
            applyShardToRouteReassignment(plan);
        } else {
            System.out.println("Shard Assignment is not modified after optimization.");
        }


        return plan.toString();
    }

    void applyShardToRouteReassignment(ShardReassignmentPlan plan) throws TException{
        for(ShardReassignment reassignment: plan.getReassignmentList()) {
            master.reassignBucketToRoute(reassignment.taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
        }
    }
}
