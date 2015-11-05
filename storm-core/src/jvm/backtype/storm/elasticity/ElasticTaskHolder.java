package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    public static Logger LOG = LoggerFactory.getLogger(ElasticTaskHolder.class);

    public ElasticTaskHolder() {
        LOG.info("ElasticTaskHolder is launched!");
    }
}
