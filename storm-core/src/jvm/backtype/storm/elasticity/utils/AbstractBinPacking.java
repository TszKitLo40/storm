package backtype.storm.elasticity.utils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBinPacking {

    protected List<Ball> balls;
    protected long binSize;

    public AbstractBinPacking(List<Long> in, long binSize) {
        balls = new ArrayList<>(in.size());
        for(int i = 0; i < in.size(); i++) {
            balls.add(new Ball(i, in.get(i)));
        }
//        this.balls = in;
        this.binSize = binSize;
    }

    /**
     * runs algorithm and returns minimum number of needed bins.
     *
     * @return minimum number of needed bins
     */
    public abstract int getResult();

    /**
     * print the best bin packing determined by getResult().
     */
    public abstract void printBestBins();

    public List<Bin> deepCopy(List<Bin> bins) {
        ArrayList<Bin> copy = new ArrayList<Bin>();
        for (int i = 0; i < bins.size(); i++) {
            copy.add(bins.get(i).deepCopy());
        }
        return copy;
    }
}
