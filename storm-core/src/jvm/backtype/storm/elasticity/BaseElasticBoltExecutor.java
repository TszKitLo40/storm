package backtype.storm.elasticity;

import backtype.storm.elasticity.utils.KeyBucketSampler;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RateTracker;
import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/4/15.
 */
public class BaseElasticBoltExecutor implements IRichBolt {

//    static final long serialVersionUID = -3216586099702029175L;

    public static Logger LOG = LoggerFactory.getLogger(BaseElasticBoltExecutor.class);

    private BaseElasticBolt _bolt;

    private transient ElasticOutputCollector _outputCollector;

    private transient OutputCollector _originalCollector;

    private transient LinkedBlockingQueue<TupleExecuteResult> _resultQueue;

    private transient Thread _resultHandleThread;

    private transient int _taskId;

    private transient ElasticTasks _elasticTasks;
    private transient ElasticTaskHolder _holder;

    private transient RateTracker _rateTracker;

    public transient KeyBucketSampler _keyBucketSampler;

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
                    LOG.debug("an execution result is emit!");
                }
            } catch (InterruptedException ie) {
                LOG.info("ResultHandler is interrupted!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void handle(TupleExecuteResult result) {
//            System.out.println("Tuple content: "+result._streamId + " " + result._inputTuple + " "+ result._outputTuple);
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
        _keyBucketSampler = new KeyBucketSampler(8);
        _resultQueue = new LinkedBlockingQueue<>(256);
        _outputCollector = new ElasticOutputCollector(_resultQueue);
        _bolt.prepare(stormConf, context);
        _originalCollector = collector;
        _resultHandleThread = new Thread(new ResultHandler()) ;
        _resultHandleThread.start();
//        _elasticTasks = new ElasticTasks(_bolt);
//        _elasticTasks.prepare(_outputCollector);
        _taskId = context.getThisTaskId();
        _elasticTasks = ElasticTasks.createHashRouting(1,_bolt,_taskId, _outputCollector);
//        createTest();
//        _elasticTasks = ElasticTasks.createVoidRouting(_bolt, _taskId, _outputCollector);
        _rateTracker = new RateTracker(10000, 5);
        _holder = ElasticTaskHolder.instance();
        if(_holder!=null) {
            _holder.registerElasticBolt(this, _taskId);
        }
    }

    @Override
    public void execute(Tuple input) {

        final Object key = _bolt.getKey(input);
        _keyBucketSampler.record(key);

        if(!_elasticTasks.tryHandleTuple(input,key)) {
            System.err.println("elastic task fails to process a tuple!");
            assert(false);
        }
//
//        if(_elasticTasks==null||!_elasticTasks.tryHandleTuple(input,key))
//            _bolt.execute(input, _outputCollector);
        _rateTracker.notify(1);

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

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }

    public ElasticTasks get_elasticTasks() {
        return _elasticTasks;
    }

    private void createTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        System.out.print("Started!");
                        Thread.sleep(10000);
                        LOG.info("Before setting! P="+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        System.out.format("Before setting! P=%d", _elasticTasks.get_routingTable().getNumberOfRoutes());
                        LOG.info("After setting! P="+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        _elasticTasks.setHashRouting(new Random().nextInt(10)+1);
                        _holder.sendMessageToMaster("Task["+_taskId+"] changed is parallelism to "+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        System.out.format("After setting! P=%d", _elasticTasks.get_routingTable().getNumberOfRoutes());
                    }
                } catch (Exception e) {

                }
            }
        }).start();
        System.out.println("testing thread is created!");
    }

    public void insertToResultQueue(TupleExecuteResult result) {
        try {
            _resultQueue.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public double getRate() {
        return _rateTracker.reportRate();
    }
}
