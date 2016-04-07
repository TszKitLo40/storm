package backtype.storm.elasticity.metrics;

/**
 * Created by robert on 4/7/16.
 */
public class ElasticExecutorMetrics {

    ExecutionLatencyForRoutes executionLatencyForRoutes = new ExecutionLatencyForRoutes();

    public void updateLatency(ExecutionLatencyForRoutes latencyForRoutes) {
        executionLatencyForRoutes.merge(latencyForRoutes);
    }

    public Long getAverageLatency() {
        return executionLatencyForRoutes.getAverageLatench();
    }
}
