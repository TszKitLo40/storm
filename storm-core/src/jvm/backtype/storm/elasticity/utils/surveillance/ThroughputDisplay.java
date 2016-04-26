package backtype.storm.elasticity.utils.surveillance;

import java.io.PrintStream;
import java.util.Map;

/**
 * Created by robert on 10/29/15.
 */
public class ThroughputDisplay {

    private Surveillant surveillant;

    public ThroughputDisplay(Surveillant s) {
        surveillant = s;
    }

    public void flushResult(PrintStream out) {
        Map<String, Double> throughputs = surveillant.getThroughputs();
        double sum = 0;
        for(Double v: throughputs.values()) {
            sum += v;
        }
        out.format("Throughput: %f\n",sum);
    }
}
