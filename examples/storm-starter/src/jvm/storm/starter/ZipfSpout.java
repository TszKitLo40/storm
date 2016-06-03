package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.generated.MasterService;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import storm.starter.surveillance.ThroughputMonitor;

import java.util.Map;


/**
 * Created by acelzj on 03/05/16.
 */
public class ZipfSpout extends BaseRichSpout implements ChangeDistributionService.Iface{
    SpoutOutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    static ZipfSpout _instance;
    long _seed;
    long _sleepTimeInMilics;
    Thread _changeDistributionThread;

    public ZipfSpout(){

    }
    public class ChangeDistribution implements Runnable {
        public void run() {
            while (true) {
                Utils.sleep(_sleepTimeInMilics);
                _seed = System.currentTimeMillis();
                _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), String.valueOf(_seed)));
            }
        }
    }

    public void changeNumberOfElements(int numberofElements) throws TException{
        _numberOfElements = numberofElements;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), String.valueOf(_seed)));
    }

    public void changeExponent(double exponent) throws TException{
        _exponent = exponent;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), String.valueOf(_seed)));
    }

    public void createThriftServiceThread() {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ChangeDistributionService.Processor processor = new ChangeDistributionService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9080);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
                    Slave.getInstance().logOnMaster("Hello");
                   // log("Starting the changeDistribution daemon...");
                    server.serve();
                    Slave.getInstance().logOnMaster("ThriftServiceThread.started");
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
        _numberOfElements = 1000;
        _exponent = 0.75;
        _instance = this;
        _sleepTimeInMilics = 60000;
        //   createThriftServiceThread();
        _seed = System.currentTimeMillis();
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), String.valueOf(_seed)));
        _changeDistributionThread = new Thread(new ChangeDistribution());
        _changeDistributionThread.start();
       // createThriftServiceThread();
        System.out.println("emited");
   //     _changeDistributionThread = new Thread(new ChangeDistribution());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfElements", "exponent", "seed"));
    }

    public void nextTuple(){

    }
}
