package backtype.storm.elasticity.metrics;

import backtype.storm.elasticity.message.actormessage.IMessage;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by robert on 4/7/16.
 */
public class ExecutionLatencyForRoutes  implements Serializable {

    private Map<Integer, Long> routeToLatency = new HashMap<>();

    transient private Map<Integer, Long> routeToUpdateTime = new HashMap<>();

    public void add(int routeId, long latency) {
        routeToLatency.put(routeId, latency);
        routeToUpdateTime.put(routeId, System.currentTimeMillis());
    }

    public void merge(ExecutionLatencyForRoutes latencyForRoutes) {
        routeToLatency.putAll(latencyForRoutes.routeToLatency);
        for(Integer route: latencyForRoutes.routeToLatency.keySet()) {
            routeToUpdateTime.put(route, System.currentTimeMillis());
        }
    }

    public Long getRecentAverageLatency(int ms) {
        if(routeToLatency.isEmpty())
            return null;

        long sum = 0;
        long count = 0;
        for(int route: routeToLatency.keySet()) {
            if(System.currentTimeMillis() - routeToUpdateTime.get(route) < ms) {
                sum += routeToLatency.get(route);
                count++;
            }
        }

        if(count != 0)
            return sum/routeToLatency.size();
        return null;
    }

    public Long getAverageLatency() {
        if(routeToLatency.isEmpty()) {
            return null;
        }
        long sum = 0;
        for(long latency: routeToLatency.values()) {
            sum += latency;
        }
        return sum/routeToLatency.size();
    }

    public String toString() {
        String str = "ExecutionLatencyForRoutes:\n";
        for(Integer route: routeToLatency.keySet()) {
            str += route+": " + routeToLatency.get(route) +"\n";
        }
        return str;
    }

    public void removeInvalidRoutes(Set<Integer> validRoutes) {
        for(Integer route: routeToLatency.keySet()) {
            if(!validRoutes.contains(route)) {
                routeToLatency.remove(route);
            }
            System.out.println(String.format("latency for route %d is removed.", route));
        }
    }
}
