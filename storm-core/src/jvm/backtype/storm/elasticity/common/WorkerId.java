package backtype.storm.elasticity.common;

import java.io.Serializable;

/**
 * Created by robert on 11/20/15.
 */
public class WorkerId implements Serializable {

    String ip;
    int port;

    public WorkerId(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public WorkerId(int port) {
        this("Unknown", port);
    }
}
