package storm.starter; /**
 * Created by acelzj on 03/05/16.
 */

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.util.ComputationSimulator;

import java.util.List;
import java.util.Map;

public class ResourceCentricComputationBolt extends BaseElasticBolt{
    int sleepTimeInMilics;

    List<Integer> upstreamTaskIds;

    int receivedMigrationCommand;

    public ResourceCentricComputationBolt(int sleepTimeInSecs) {
        this.sleepTimeInMilics = sleepTimeInSecs;
    }

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        System.out.println("execute");
//        utils.sleep(sleepTimeInMilics);
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals(Utils.DEFAULT_STREAM_ID)) {
            ComputationSimulator.compute(sleepTimeInMilics * 1000000);
            String number = tuple.getString(0);
            Integer count = (Integer) getValueByKey(number);
            if (count == null)
                count = 0;
            count++;
            setValueByKey(number, count);
            collector.emit(tuple, new Values(number, count));
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateMigrationCommandStream)) {
            receivedMigrationCommand++;
            Slave.getInstance().logOnMaster(String.format("Received StateMigrationCommand"));
            if(receivedMigrationCommand==upstreamTaskIds.size()) {
                // received the migration command from each of the upstream tasks.
                receivedMigrationCommand = 0;
                int sourceTaskOffset = tuple.getInteger(0);
                int targetTaskOffset = tuple.getInteger(1);
                int shardId = tuple.getInteger(2);
                KeyValueState state = getState();

                state.getState().put("key", new byte[1024 * 1024 * 32]);

                Slave.getInstance().logOnMaster("State migration starts!");
                collector.emit(ResourceCentricZipfComputationTopology.StateMigrationStream, tuple, new Values(sourceTaskOffset, targetTaskOffset, shardId, state));
            }
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateUpdateStream)) {
            Slave.getInstance().logOnMaster("Recieved new state!");
            int targetTaskOffset = tuple.getInteger(0);
            KeyValueState state = (KeyValueState) tuple.getValue(1);
            getState().update(state);
            Slave.getInstance().logOnMaster("State is updated!");
            collector.emit(ResourceCentricZipfComputationTopology.StateReadyStream, tuple, new Values(targetTaskOffset));

        }
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "count"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateMigrationStream, new Fields("sourceTaskId", "targetTaskId", "shardId", "state"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateReadyStream, new Fields("targetTaskId"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
        upstreamTaskIds = context.getComponentTasks("generator");
        receivedMigrationCommand = 0;
    }

    @Override
    public Object getKey(Tuple tuple) {
        return tuple.getValue(0);
    }

}
