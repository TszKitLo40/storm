package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Master;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.actormessage.RoutingCreatingCommand;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.generated.TaskNotExistException;

/**
 * Created by Robert on 11/11/15.
 */
public class Scheduler {

    Master master;

    static Scheduler instance;

    public Scheduler() {
        master = Master.createActor();
        instance = this;
    }

    static public Scheduler getInstance() {
        return instance;
    }

    public String optimizeBucketToRoutingMapping(int taskId) throws TaskNotExistException, RoutingTypeNotSupportedException {
        // 1. get routingTable

        RoutingTable routingTable = master.getRoutingTable(taskId);
        BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
        if(balancedHashRouting == null) {
            throw new RoutingTypeNotSupportedException("Only support BalancedHashRouting family routing table!");
        }



        // 2. get Distribution;

//        Histograms histograms = master.getDistributionHistogram(taskId);

        return balancedHashRouting.toString();
    }

}
