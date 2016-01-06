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

    public RouteId(int taskid, int route) {
        TaskId = taskid;
        Route = route;
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof RouteId) {
            final RouteId routeId = (RouteId) o;
            if(routeId.Route == Route && routeId.TaskId == TaskId)
                return true;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return TaskId * 101 + Route *7;
    }

    public String toString() {
        return TaskId + "." + Route;
    }
}
