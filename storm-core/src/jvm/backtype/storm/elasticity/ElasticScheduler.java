package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Master;

/**
 * Created by Robert on 11/11/15.
 */
public class ElasticScheduler {

    Master master;

    public ElasticScheduler() {
        master = Master.createActor();
    }
}
