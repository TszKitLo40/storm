package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by robert on 13/6/16.
 */
public class ZipControlerBolt implements IRichBolt {

    OutputCollector collector;

    long[] generatorProgress;

    long progress = 0;

    int progressTolerance = 50;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        generatorProgress = new long[context.getComponentTasks("generator").size()];
    }

    @Override
    public void execute(Tuple input) {
        int taskid = input.getInteger(0);
        long localProgress = input.getLong(1);
        generatorProgress[taskid] = localProgress;
        long min = Long.MAX_VALUE;
        int i = 0;
        for(long p: generatorProgress) {
            min = Math.min(p, min);
//                Slave.getInstance().logOnMaster(String.format("Progress of Task %d: %d", i++, p ));
        }
//            Slave.getInstance().logOnMaster(String.format("progress: %d, min : %d", progress, min));
        if(min > progress) {
            progress = min;
//                Slave.getInstance().logOnMaster(String.format("CountPermission is updated to %d", progress + progressTolerance));
            collector.emit("CountPermissionStream", new Values(progress + progressTolerance));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("CountPermissionStream", new Fields("progress"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
