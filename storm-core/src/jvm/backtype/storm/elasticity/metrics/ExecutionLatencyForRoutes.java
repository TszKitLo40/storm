package backtype.storm.elasticity.metrics;

import backtype.storm.elasticity.message.actormessage.IMessage;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 4/7/16.
 */
public class ExecutionLatencyForRoutes  implements Serializable {

    private Map<Integer, Long> routeToLatency = new HashMap<>();

    public void add(int routeId, long latency) {
        routeToLatency.put(routeId, latency);
    }

    public void merge(ExecutionLatencyForRoutes latencyForRoutes) {
        routeToLatency.putAll(latencyForRoutes.routeToLatency);
    }

    public Long getAverageLatench() {
        if(routeToLatency.isEmpty()) {
            return null;
        }
        long sum = 0;
        for(long latency: routeToLatency.values()) {
            sum += latency;
        }
        return sum/routeToLatency.size();
    }
}
