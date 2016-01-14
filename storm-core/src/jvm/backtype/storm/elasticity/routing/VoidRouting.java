package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.util.ArrayList;

/**
 * Created by Robert on 11/4/15.
 */
public class VoidRouting implements RoutingTable {
    @Override
    public int route(Object key) {
        return origin;
    }

    @Override
    public int getNumberOfRoutes() {
        return 0;
    }

    @Override
    public ArrayList<Integer> getRoutes() {
        return null;
    }

    @Override
    public Histograms getRoutingDistribution() {
        return null;
    }

    @Override
    public void enableRoutingDistributionSampling() {

    }
}
