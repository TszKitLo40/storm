package backtype.storm.elasticity;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/4/15.
 */
public class BaseElasticBoltExecutor implements IRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(BaseElasticBoltExecutor.class);

    private BaseElasticBolt _bolt;

    private transient ElasticOutputCollector _outputCollector;

    private transient OutputCollector _originalCollector;

    private transient LinkedBlockingQueue<TupleExecuteResult> _resultQueue;

    private transient Thread _resultHandleThread;

    private transient ElasticTasks _elasticTasks;

    public BaseElasticBoltExecutor(BaseElasticBolt bolt) {
        _bolt = bolt;
    }

    public class ResultHandler implements Runnable {

        @Override
        public void run() {
            try {
                while(true) {
                    TupleExecuteResult result = _resultQueue.take();
                    handle(result);
                }
            } catch (InterruptedException e) {
                LOG.info("ResultHandler is interrupted!");
            }
        }

        private void handle(TupleExecuteResult result) {
            switch (result._commandType) {
                case TupleExecuteResult.Emit:
                    _originalCollector.emit(result._streamId, result._inputTuple, result._outputTuple);
                    break;
                default:
                    assert(false);
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _resultQueue = new LinkedBlockingQueue<>(1024*1024);
        _outputCollector = new ElasticOutputCollector(_resultQueue);
        _bolt.prepare(stormConf, context);
        _originalCollector = collector;
        _resultHandleThread = new Thread(new ResultHandler()) ;
        _resultHandleThread.start();
        _elasticTasks = ElasticTasks.createHashRouting(3,_bolt, _outputCollector);
//        createTest();
        ElasticTaskHolder holder = ElasticTaskHolder.instance();
        if(holder!=null) {
            int taskId = context.getThisTaskId();
            holder.registerElasticBolt(this, taskId);
        }
    }

    @Override
    public void execute(Tuple input) {

        if(_elasticTasks!=null && _elasticTasks._routingTable!=null) {
            int route=_elasticTasks._routingTable.route(_bolt.getKey(input));
            if(route != RoutingTable.origin) {
                _elasticTasks.handleTuple(input,_bolt.getKey(input));
                return;
            }
        }
        _bolt.execute(input, _outputCollector);
    }

    @Override
    public void cleanup() {
        _bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void createTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        System.out.print("Started!");
                        Thread.sleep(1000);
                        LOG.info("Before setting! P="+_elasticTasks._routingTable.getNumberOfRoutes());
                        System.out.format("Before setting! P=%d", _elasticTasks._routingTable.getNumberOfRoutes());
                        LOG.info("After setting! P="+_elasticTasks._routingTable.getNumberOfRoutes());
                        _elasticTasks.setHashRouting(new Random().nextInt(10)+1, _bolt, _outputCollector);
                        System.out.format("After setting! P=%d", _elasticTasks._routingTable.getNumberOfRoutes());
                    }
                } catch (Exception e) {

                }
            }
        }).start();
        System.out.println("testing thread is created!");
    }
}
