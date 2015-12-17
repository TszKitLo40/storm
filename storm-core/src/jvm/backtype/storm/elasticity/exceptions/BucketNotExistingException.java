package backtype.storm.elasticity.exceptions;

/**
 * Created by robert on 12/16/15.
 */
public class BucketNotExistingException extends Exception {

    public BucketNotExistingException(String str) {
        super(str);
    }
}
