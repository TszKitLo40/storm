package backtype.storm.elasticity.utils.timer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/30/15.
 */
public class GeneralTimer {

    Map<String, Duration> eventTimers = new HashMap<>();
    String name;
    public GeneralTimer(String name) {
        this.name = name;
    }

    public void start(String event) {
        eventTimers.put(event, new Duration());
    }

    public void stop(String event) {
        if(!eventTimers.containsKey(event)) {
            System.err.println("You might forget to call start for " + event);
            return;
        }
        eventTimers.get(event).setEnd();
    }

    public String toString() {
        String ret = "";
        for(String e: eventTimers.keySet()) {
            ret += e +": " + eventTimers.get(e).getDuration() +"  ";

        }
        return ret;
    }

}
