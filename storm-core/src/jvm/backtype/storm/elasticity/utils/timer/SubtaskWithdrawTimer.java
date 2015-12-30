package backtype.storm.elasticity.utils.timer;


/**
 * Created by robert on 12/29/15.
 */
public class SubtaskWithdrawTimer {
    static SubtaskWithdrawTimer instance;

    long start;
    long prepared;
    long terminated;

    public static SubtaskWithdrawTimer getInstance() {
        if(instance == null) {
            instance = new SubtaskWithdrawTimer();
        }
        return instance;
    }

    public void start() {
        start = System.currentTimeMillis();
    }

    public void prepared() {
        prepared = System.currentTimeMillis();
    }

    public void terminated() {
        terminated = System.currentTimeMillis();
    }

    public String toString() {
        String ret = "";
        ret += "Prepare: " + (prepared - start) + "\tTerminated: " + (terminated - start) + "\tTotal: " + (terminated - start) ;
        return ret;
    }



}
