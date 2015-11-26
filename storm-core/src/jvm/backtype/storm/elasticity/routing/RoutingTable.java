package backtype.storm.elasticity.routing;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Robert on 11/3/15.
 */
public interface RoutingTable extends Serializable {
    public static int origin = 0;
    public static int remote = -2;
    public int route(Object key);
    public int getNumberOfRoutes();
    public ArrayList<Integer> getRoutes();
    


}
