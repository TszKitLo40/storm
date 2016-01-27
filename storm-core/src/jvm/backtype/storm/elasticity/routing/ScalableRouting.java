package backtype.storm.elasticity.routing;

/**
 * Created by robert on 1/27/16.
 */
public interface ScalableRouting {

    int scalingOut();
    void scalingIn();
}
