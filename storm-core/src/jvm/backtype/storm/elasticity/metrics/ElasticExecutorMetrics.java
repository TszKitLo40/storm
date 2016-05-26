package backtype.storm.elasticity.metrics;

import backtype.storm.elasticity.actors.Slave;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by robert on 4/7/16.
 */
public class ElasticExecutorMetrics {

    ExecutionLatencyForRoutes executionLatencyForRoutes = new ExecutionLatencyForRoutes();

    public void updateLatency(ExecutionLatencyForRoutes latencyForRoutes) {
        executionLatencyForRoutes.merge(latencyForRoutes);
    }

    public Long getRecentAverageLatency(int ms) {
        return executionLatencyForRoutes.getRecentAverageLatency(ms);
    }

    public Long getAverageLatency() {
        Slave.getInstance().sendMessageToMaster(executionLatencyForRoutes.toString());
        return executionLatencyForRoutes.getAverageLatency();
    }

    public void removeInvalidRoutes(List<Integer> validRoutes) {
        Set<Integer> validRouteSet = new HashSet<>();
        validRouteSet.addAll(validRoutes);
        executionLatencyForRoutes.removeInvalidRoutes(validRouteSet);
    }
}
