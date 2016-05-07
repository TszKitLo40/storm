package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricGeneratorBolt implements IRichBolt{

    ZipfDistribution _distribution;
    OutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    Thread _emitThread;
    transient ThroughputMonitor monitor;
    transient BalancedHashRouting routingTable;

    private int numberOfComputingTasks;
    private List<Integer> downStreamTaskIds;

    private int taskId;

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
            try {
                Random random = new Random();
                while (true) {

                    Thread.sleep(32);
//                    int key = _distribution.sample();
                      int key = random.nextInt(_numberOfElements);
//                    System.out.println("key");
//                    System.out.println(key);

//                    _collector.emit(new Values(String.valueOf(key)));
                    int pos = routingTable.route(String.valueOf(key));
                    int targetTaskId = downStreamTaskIds.get(pos);
                    _collector.emitDirect(targetTaskId, new Values(String.valueOf(key)));
                    monitor.rateTracker.notify(1);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfTask"));
        declarer.declareStream("statics", new Fields("taskId", "Histogram"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateMigrationCommandStream, new Fields("sourceTaskId","targetTaskId", "shardId"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.FeedbackStream, new Fields("command", "arg1"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        this.taskId = context.getThisTaskId();

        downStreamTaskIds = context.getComponentTasks(ResourceCentricZipfComputationTopology.ComputationBolt);

        numberOfComputingTasks = downStreamTaskIds.size();

        routingTable = new BalancedHashRouting(numberOfComputingTasks);

        _distribution = new ZipfDistribution(10240, 0.001);

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
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.UpstreamCommand)) {
            String command  = tuple.getString(0);
            if(command.equals("getHistograms")) {
                _collector.emit("statics", new Values(taskId, routingTable.getBucketsDistribution()));
            } else if (command.equals("pausing")) {
                Slave.getInstance().logOnMaster("Received pausing command on " + taskId);
                int sourceTaskOffset = tuple.getInteger(1);
                int targetTaskOffset = tuple.getInteger(2);
                int shardId = tuple.getInteger(3);
                _emitThread.interrupt();
                try {
                    _emitThread.join();
                    Slave.getInstance().logOnMaster("Sending thread is paused on " + taskId);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                routingTable.reassignBucketToRoute(shardId, targetTaskOffset);
                Slave.getInstance().logOnMaster("Routing table is updated on " + taskId);
                _collector.emitDirect(downStreamTaskIds.get(sourceTaskOffset), ResourceCentricZipfComputationTopology.StateMigrationCommandStream, new Values(sourceTaskOffset, targetTaskOffset, shardId));
            } else if (command.equals("resuming")) {
                int sourceTaskIndex = tuple.getInteger(1);
                _emitThread = new Thread(new emitKey());
                _emitThread.start();

                Slave.getInstance().logOnMaster("Routing thread is resumed!");
                _collector.emit(ResourceCentricZipfComputationTopology.FeedbackStream, new Values("resumed", sourceTaskIndex));
            }
        }
    }

}
