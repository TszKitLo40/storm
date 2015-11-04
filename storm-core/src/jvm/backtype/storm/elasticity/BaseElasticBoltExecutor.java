package backtype.storm.elasticity;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.common.util.concurrent.Runnables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/4/15.
 */
public class BaseElasticBoltExecutor implements IRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(BaseElasticBoltExecutor.class);

    private BaseElasticBolt _bolt;

    private transient ElasticOutputBuffer _outputBuffer;

    private transient OutputCollector _collector;

    private transient LinkedBlockingQueue<TupleExecuteResult> _resultQueue;

    private transient Thread _resultHandleThread;

    private transient RoutingTable _routingTable;

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
                    _collector.emit(result._streamId, result._inputTuple, result._outputTuple);
                    break;
                default:
                    assert(false);
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _resultQueue = new LinkedBlockingQueue<>(1024*1024);
        _outputBuffer = new ElasticOutputBuffer(_resultQueue);
        _bolt.prepare(stormConf, context);
        _collector = collector;
        _resultHandleThread = new Thread(new ResultHandler()) ;
        _resultHandleThread.start();
    }

    @Override
    public void execute(Tuple input) {

        if(_routingTable!=null) {
            int route=_routingTable.route(_bolt.getKey(input));
            if(route == RoutingTable.origin) {
                _bolt.execute(input,_outputBuffer);
            } else {

            }
        }
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
}
