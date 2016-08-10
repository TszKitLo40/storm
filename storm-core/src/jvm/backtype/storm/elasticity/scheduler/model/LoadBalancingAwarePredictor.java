package backtype.storm.elasticity.scheduler.model;

import backtype.storm.elasticity.scheduler.ElasticScheduler;

/**
 * Created by robert on 16-8-10.
 */
public class LoadBalancingAwarePredictor implements ExecutorParallelismPredictor {
    final double overProvisionFactor = 0.5;
    @Override
    public int predict(Double inputRate, int currentDop, Double ratePerTask, long[] routeLoads, long maxShardLoad) {
        inputRate = inputRate * (currentDop + 0.5) / currentDop;
        Double performanceFactor = ElasticScheduler.getPerformanceFactor(routeLoads);
        Double processCapability = ratePerTask * currentDop * performanceFactor;

        int desirableDoP = (int)Math.ceil(inputRate / processCapability * currentDop);

        if(desirableDoP > currentDop) {
            //When the shard with the highest workload is assigned to the most overloaded task, neither load balancing can
            // be improved by shard reassignments nor the processing capability can be enhanced by increasing the parallelism.
            // In such case, we should not scale up.
            long maxRouteLoads = Long.MIN_VALUE;
            for (long i : routeLoads) {
                maxRouteLoads = Math.max(i, maxRouteLoads);
            }

            if(maxRouteLoads == maxShardLoad)
                desirableDoP = currentDop;
        }

        return desirableDoP;
    }
}
