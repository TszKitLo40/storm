package backtype.storm.elasticity.utils.timer;

/**
 * Created by robert on 12/30/15.
 */
public class Duration {

    long start;
    long end;

    public Duration() {
        start = System.currentTimeMillis();
    }

    public void setEnd() {
        end = System.currentTimeMillis();
    }

    public long getDuration() {
        return end - start;
    }
}
