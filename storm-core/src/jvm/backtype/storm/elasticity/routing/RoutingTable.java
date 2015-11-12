package backtype.storm.elasticity.routing;

import backtype.storm.state.KeyValueState;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;

/**
 * Created by Robert on 11/3/15.
 */
public interface RoutingTable {
    public static int origin = -1;
    public int route(Object key);
    public int getNumberOfRoutes();
    public ArrayList<Integer> getRoutes();


}
