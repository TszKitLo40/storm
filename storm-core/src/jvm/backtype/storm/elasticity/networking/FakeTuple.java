package backtype.storm.elasticity.networking;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by robert on 12/9/15.
 */
public class FakeTuple implements Serializable {

    Integer[] integers;

    public FakeTuple() {
        int rand = new Random().nextInt(200)+200;
        integers = new Integer[Math.abs(rand)];
        integers = new Integer[new Random().nextInt(Math.abs(integers.length))];
    }

    public String toString() {
        return integers.length + "";
    }
}