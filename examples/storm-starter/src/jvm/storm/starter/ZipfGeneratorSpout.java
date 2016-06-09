package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.TupleExecuteResult;
import backtype.storm.elasticity.actors.Slave;
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
import java.util.Random;
/**
 * Created by acelzj on 19/04/16.
 */
public class ZipfGeneratorSpout extends BaseRichSpout{
    int _emit_cycles;
    private int count = 0;
    SpoutOutputCollector _collector;
    ZipfDistribution _distribution;
    private transient Thread _changeDistributionThread;
    transient ThroughputMonitor monitor;
    int _prime;
    final int[] primes = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271};
    public class ChangeDistribution implements Runnable {

        @Override
        public void run() {
            while (true) {
                    Utils.sleep(15000);
//                    long seed = System.currentTimeMillis();
                    Random rand = new Random(1);
                    System.out.println("distribution has been changed");
                    _prime = primes[rand.nextInt(primes.length)];
                    Slave.getInstance().logOnMaster("distribution has been changed");
                }
            }
    }

    public ZipfGeneratorSpout(int emit_cycles){
        _emit_cycles = emit_cycles;
        _prime = 41;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
        monitor = new ThroughputMonitor(""+context.getThisTaskId());
        _distribution = new ZipfDistribution(100, 1);
        _changeDistributionThread = new Thread(new ChangeDistribution());
        _changeDistributionThread.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("worker_nums"));
    }

    public void nextTuple(){
        Utils.sleep(_emit_cycles);
     //   System.out.println("sample");
       // System.out.println(_distribution.sample());
        int key = _distribution.sample();
        key = ((key + _prime) * 101) % 1113;
        _collector.emit(new Values(String.valueOf(key)), new Object());

        ++count;
        monitor.rateTracker.notify(1);
//            System.out.format("sent %d %d ms\n",count,System.currentTimeMillis() - start);
    }

}
