package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.surveillance.ThroughputMonitor;

import java.io.FileNotFoundException;
import org.apache.commons.math3.distribution.ZipfDistribution;
import java.util.*;
/**
 * Created by acelzj on 19/04/16.
 */
public class ZipfGeneratorSpout extends BaseRichSpout{
    int _N;
    int _s;
    int _emit_cycles;
    ZipfDistribution _distribution;
    private int count = 0;
    SpoutOutputCollector _collector;
    public ZipfGeneratorSpout(int emit_cycles){
        _N = 100000;
        _s = 2;
        _emit_cycles = emit_cycles;
        _distribution = new ZipfDistribution(_N, _s);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
//            monitor = new ThroughputMonitor(""+context.getThisTaskId());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("worker_nums"));
    }

    public void nextTuple(){
        Utils.sleep(_emit_cycles);
        System.out.println("sample");
       // System.out.println(_distribution.sample());
        _collector.emit(new Values(String.valueOf(_distribution.sample())));
        ++count;
//            monitor.rateTracker.notify(1);
//            System.out.format("sent %d %d ms\n",count,System.currentTimeMillis() - start);
    }

}
