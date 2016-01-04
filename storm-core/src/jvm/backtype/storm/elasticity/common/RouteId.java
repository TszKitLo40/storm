package backtype.storm.elasticity.common;

/**
 * Created by robert on 1/4/16.
 */
public class RouteId {

    public int TaskId;

    public int Route;

    public RouteId(String xdy) {
        TaskId = Integer.parseInt(xdy.split("\\.")[0]);
        Route = Integer.parseInt(xdy.split("\\.")[1]);
    }
}
