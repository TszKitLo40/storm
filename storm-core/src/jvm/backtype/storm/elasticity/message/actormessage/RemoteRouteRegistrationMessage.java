package backtype.storm.elasticity.message.actormessage;

import java.util.ArrayList;

/**
 * Created by Robert on 11/19/15.
 */
public class RemoteRouteRegistrationMessage implements IMessage {

    public int taskid;
    public ArrayList<Integer> routes;
    public String host;
    public boolean unregister = false;

    public RemoteRouteRegistrationMessage(int taskid, int route, String host) {
        ArrayList<Integer> routes =  new ArrayList<>();
        routes.add(route);
        this.taskid = taskid;
        this.routes = routes;
        this.host = host;
    }

    public RemoteRouteRegistrationMessage(int taskid, ArrayList<Integer> routes, String host) {
        this.taskid = taskid;
        this.routes = routes;
        this.host = host;
    }

    public void setUnregister() {
        unregister = true;
    }
}
