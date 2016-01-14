package storm.starter.util;

import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class ComputationSimulator {
    public static long compute(long timeInNanoSecond) {
        final long start = System.nanoTime();
        long seed = start;
        while(System.nanoTime() - start < timeInNanoSecond) {
            seed = (long) Math.sqrt(new Random().nextInt());
        }
        return seed;
    }
}
