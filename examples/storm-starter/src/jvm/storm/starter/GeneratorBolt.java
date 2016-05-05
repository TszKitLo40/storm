package storm.starter;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.utils.Utils;

import java.util.Map;
import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.math3.distribution.ZipfDistribution;
/**
 * Created by acelzj on 03/05/16.
 */
public class GeneratorBolt implements IRichBolt{

    ZipfDistribution _distribution;
    OutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    Thread _emitThread;
    transient ThroughputMonitor monitor;
   // final int[] primes = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271};
   /* public class ChangeDistribution implements Runnable (Tuple tuple){

        @Override
        public void run() {
            while (true) {
                setNumberOfElements(tuple);
                Random rand = new Random(seed);
                System.out.println("distribution has been changed");
                _prime = primes[rand.nextInt(primes.length)];
                Slave.getInstance().logOnMaster("distribution has been changed");
            }
        }
    }*/
    public class emitKey implements Runnable {
        public void run() {
            while (true) {
                try {
                    Utils.sleep(1);
                    int key = _distribution.sample();
                    System.out.println("key");
                    System.out.println(key);
                    _collector.emit(new Values(String.valueOf(key)));
                    monitor.rateTracker.notify(1);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfTask"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        monitor = new ThroughputMonitor(""+context.getThisTaskId());
        _emitThread = new Thread(new emitKey());
        _emitThread.start();
    }

    public Map getComponentConfiguration(){ return new HashedMap();}

    public void setNumberOfElements(Tuple tuple) {
        System.out.println(tuple.getString(0));
        _numberOfElements = Integer.parseInt(tuple.getString(0));
    }

    public void setExponent(Tuple tuple) {
        System.out.println(tuple.getString(1));
        _exponent = Double.parseDouble(tuple.getString(1));
    }

    public void cleanup() { }

    public void execute(Tuple tuple){
      //  setNumberOfElements(tuple);
      //  setExponent(tuple);
        if(tuple.getSourceStreamId().equals(Utils.DEFAULT_STREAM_ID)) {
            _numberOfElements = Integer.parseInt(tuple.getString(0));
            _exponent = Double.parseDouble(tuple.getString(1));
            _distribution = new ZipfDistribution(_numberOfElements, _exponent);
        }

    }

}
