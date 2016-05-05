package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import storm.starter.generated.ResourceCentricControllerService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by Robert on 4/5/16.
 */
public class ResourceCentricControllerBolt implements IRichBolt, ResourceCentricControllerService.Iface {

    OutputCollector collector;

    Map<Integer, Histograms> taskToHistogram;

    BalancedHashRouting routingTable;

    List<Integer> downstreamTaskIds;

    List<Integer> upstreamTaskIds;

    Map<Integer, Semaphore> sourceTaskIdToPendingTupleCleanedSemphore = new ConcurrentHashMap<>();

    Map<Integer, Semaphore> targetTaskIdToWaitingStateMigrationSemphore = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;

        taskToHistogram = new HashMap<>();

        upstreamTaskIds = context.getComponentTasks("generator");

        downstreamTaskIds = context.getComponentTasks("computator");

        routingTable = new BalancedHashRouting(downstreamTaskIds.size());

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(1000);
                        collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("getHistograms", 0, 0, 0));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        createThriftThread(this);
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();
        if(streamId.equals("statics")) {
            int sourceTaskId = input.getInteger(0);
            Histograms histograms = (Histograms)input.getValue(1);
            taskToHistogram.put(sourceTaskId, histograms);
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateMigrationStream)) {
            int sourceTaskOffset = input.getInteger(0);
            int targetTaskOffset = input.getInteger(1);
            int shardId = input.getInteger(2);
            KeyValueState state = (KeyValueState) input.getValue(3);
            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskOffset).release();
            collector.emitDirect(downstreamTaskIds.get(targetTaskOffset), ResourceCentricZipfComputationTopology.StateUpdateStream, new Values(targetTaskOffset, state));
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateReadyStream)) {
            int targetTaskOffset = input.getInteger(0);
            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskOffset).release();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ResourceCentricZipfComputationTopology.UpstreamCommand, new Fields("Command", "arg1", "arg2", "arg3"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateUpdateStream, new Fields("targetTaskId", "state"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

//    @Override
//    public void shardReassignment(int sourceTaskIndex, int targetTaskIndex, int shardId) throws TException {
//        try {
//            if(sourceTaskIndex >= downstreamTaskIds.size())
//                return;
//            if(targetTaskIndex >= downstreamTaskIds.size())
//                return;
//            if(shardId > Config.NumberOfShard)
//                return;
//
//            int sourceTaskId = downstreamTaskIds.get(sourceTaskIndex);
//
//            sourceTaskIdToPendingTupleCleanedSemphore.put(sourceTaskId, new Semaphore(0));
//
//            collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("pausing", sourceTaskId, targetTaskIndex, shardId));
//
//            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskId).acquire();
//
//            targetTaskIdToWaitingStateMigrationSemphore.put(targetTaskIndex, new Semaphore(0));
//
//            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskIndex).acquire();
//
//            Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d from %d to %d is ready!", shardId, sourceTaskId, targetTaskIndex));
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    private void createThriftThread(final ResourceCentricControllerBolt bolt) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ResourceCentricControllerService.Processor processor = new ResourceCentricControllerService.Processor(bolt);
//                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(19090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

//                    log("Starting the monitoring daemon...");
                    server.serve();
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    @Override
    public void shardReassignment(int sourceTaskIndex, int targetTaskIndex, int shardId) throws org.apache.thrift.TException {
        try {
            if(sourceTaskIndex >= downstreamTaskIds.size())
                return;
            if(targetTaskIndex >= downstreamTaskIds.size())
                return;
            if(shardId > Config.NumberOfShard)
                return;

            int sourceTaskId = downstreamTaskIds.get(sourceTaskIndex);

            sourceTaskIdToPendingTupleCleanedSemphore.put(sourceTaskId, new Semaphore(0));

            collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("pausing", sourceTaskId, targetTaskIndex, shardId));

            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskId).acquire();

            targetTaskIdToWaitingStateMigrationSemphore.put(targetTaskIndex, new Semaphore(0));

            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskIndex).acquire();

            Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d from %d to %d is ready!", shardId, sourceTaskId, targetTaskIndex));

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
