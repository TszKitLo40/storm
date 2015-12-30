package backtype.storm.elasticity.utils.timer;

/**
 * Created by robert on 12/29/15.
 */
public class SubtaskMigrationTimer {

    private static SubtaskMigrationTimer instance;

    long start;

    long prepare;

    long end;

    public void start() {
        start = System.currentTimeMillis();
    }

    public void prepare() {
        prepare = System.currentTimeMillis();
    }

    public void launched() {
        end = System.currentTimeMillis();
    }

    public String toString() {
        String ret = "";
        ret += "Prepare: " + (prepare - start) + "\tLaunch: " + (end - prepare) + "\tTotal: " + (end - start);
        return ret;
    }

    public static SubtaskMigrationTimer instance() {
        if(instance==null)
            instance = new SubtaskMigrationTimer();
        return instance;
    }
}
