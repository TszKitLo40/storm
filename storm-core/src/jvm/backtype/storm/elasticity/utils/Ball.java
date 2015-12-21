package backtype.storm.elasticity.utils;

import java.util.Comparator;

/**
 * Created by robert on 12/21/15.
 */
public class Ball {

    public int index;
    public long size;

    public Ball(int index, long size) {
        this.index = index;
        this.size = size;
    }

    public static class BallComparator implements Comparator<Ball> {

        @Override
        public int compare(Ball bin, Ball bin2) {
            return Long.compare(bin.size,bin2.size);
        }
    }

    public static class BallReverseComparator implements Comparator<Ball> {

        @Override
        public int compare(Ball ball, Ball ball2) {
            return Long.compare(ball2.size, ball.size);
        }
    }
}
