package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.collections.map.AbstractHashedMap;
import storm.starter.util.StringGenerator;

import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class InputGeneratorSpout extends BaseRichSpout {

    transient StringGenerator stringGenerator;
    int numberOfDistinctString;
    SpoutOutputCollector outputCollector;
    transient Random random;

    public InputGeneratorSpout(int numberOfDistinctString) {
        this.numberOfDistinctString = numberOfDistinctString;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "timestamp"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        stringGenerator = new StringGenerator(numberOfDistinctString);
        random = new Random();
        outputCollector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        outputCollector.emit(new Values((long)random.nextInt(numberOfDistinctString), System.currentTimeMillis()));
    }
}
