package backtype.storm.elasticity.utils.timer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/30/15.
 */
public class SmartTimer {

    Map<String, GeneralTimer> generalTimerMap = new HashMap<>();

    static SmartTimer instance;

    static public SmartTimer getInstance() {
        if(instance == null)
            instance = new SmartTimer();
        return instance;
    }

    public void start(String timer, String event) {
        if(!generalTimerMap.containsKey(timer)){
            generalTimerMap.put(timer, new GeneralTimer(timer));
        }
        generalTimerMap.get(timer).start(event);
    }

    public void stop(String timer, String event) {
        if(!generalTimerMap.containsKey(timer)) {
            System.err.println("You much call start before calling end!");
        }
        generalTimerMap.get(timer).stop(event);
    }

    public String getTimerString(String timer) {
        if(!generalTimerMap.containsKey(timer)) {
            return timer + " is not found!";
        }
        return generalTimerMap.get(timer).toString();
    }



}
