package storm.starter; /**
 * Created by acelzj on 03/05/16.
 */

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.util.ComputationSimulator;

import java.util.Map;
public class ComputationBolt extends BaseElasticBolt{
    int sleepTimeInMilics;

    public ComputationBolt(int sleepTimeInSecs) {
        this.sleepTimeInMilics = sleepTimeInSecs;
    }

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
        System.out.println("execute");
//        utils.sleep(sleepTimeInMilics);
        ComputationSimulator.compute(sleepTimeInMilics*1000000);
        String number = tuple.getString(0);
        Integer count = (Integer)getValueByKey(number);
        if (count == null)
            count = 0;
        count++;
        setValueByKey(number,count);
        collector.emit(tuple,new Values(number, count));
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "count"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }

    @Override
    public Object getKey(Tuple tuple) {
        return tuple.getString(0);
    }

}
