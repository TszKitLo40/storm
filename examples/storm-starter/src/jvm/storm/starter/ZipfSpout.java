package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.thrift.TException;
import storm.starter.surveillance.ThroughputMonitor;

import java.util.Map;


/**
 * Created by acelzj on 03/05/16.
 */
public class ZipfSpout extends BaseRichSpout implements ChangeDistributionService.Iface{
    SpoutOutputCollector _collector;
    int _numberOfElements;
    double _exponent;
  //  Thread _changeDistributionThread;

    public ZipfSpout(){

    }
    /*public class ChangeDistribution implements Runnable {
        public void run() {
            while (true) {
                _collector.emit(new Values(_numberOfElements, _exponent));
            }
        }
    }*/

    public void changeNumberOfElements(int numberofElements) throws TException{
        _numberOfElements = numberofElements;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent)));
    }

    public void changeExponent(double exponent) throws TException{
        _exponent = exponent;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent)));
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
        _numberOfElements = 100;
        _exponent = 1;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent)));
        System.out.println("emited");
   //     _changeDistributionThread = new Thread(new ChangeDistribution());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfElements", "exponent"));
    }

    public void nextTuple(){

    }
}
