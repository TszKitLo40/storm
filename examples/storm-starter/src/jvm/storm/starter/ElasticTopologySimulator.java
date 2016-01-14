package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.InputGeneratorSpout;

import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class ElasticTopologySimulator {
    public static class ComputationSimulator {
        public static long compute(int timeInMils) {
            final long start = System.nanoTime();
            long seed = start;
            while(System.nanoTime() - start < timeInMils) {
                seed = (long) Math.sqrt(new Random().nextInt());
            }
            return seed;
        }
    }
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseElasticBolt {

        int sleepTimeInMilics;

        public WordCount(int sleepTimeInSecs) {
            this.sleepTimeInMilics = sleepTimeInSecs;
        }

        @Override
        public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        utils.sleep(sleepTimeInMilics);
            ComputationSimulator.compute(sleepTimeInMilics);
            String word = tuple.getString(0);
            Integer count = (Integer)getValueByKey(word);
            if (count == null)
                count = 0;
            count++;
            setValueByKey(word,count);
//            collector.emit(tuple,new Values(word, count));
        }



        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public Object getKey(Tuple tuple) {
            return tuple.getString(0);
        }
    }

    public static class Printer extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
//      System.out.println(input.getString(0)+"--->"+input.getInteger(1));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name sleep-time-in-millis [debug|any other]");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new InputGeneratorSpout(8*1024), 16);

        builder.setBolt("count", new WordCount(Integer.parseInt(args[1])), 1).fieldsGrouping("spout", new Fields("word"));
//        builder.setBolt("print", new Printer(),16).globalGrouping("count");

        Config conf = new Config();
        if(args.length>2&&args[2].equals("debug"))
            conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }

    }
}

