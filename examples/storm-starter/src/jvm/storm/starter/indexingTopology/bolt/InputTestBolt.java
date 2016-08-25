package storm.starter.indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by acelzj on 7/22/16.
 */
public class InputTestBolt extends BaseRichBolt {

    private OutputCollector collector;

    private int numTuples;

    private double startTime;
    public InputTestBolt() {
        numTuples = 0;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        startTime = System.nanoTime();
    }

    public void cleanup() {
        super.cleanup();
    }

    public void execute(Tuple tuple) {
        Utils.sleep(1);
        ++numTuples;
        System.out.println(numTuples);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
