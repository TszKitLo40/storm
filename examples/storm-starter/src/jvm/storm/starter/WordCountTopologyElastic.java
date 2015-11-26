/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
import backtype.storm.utils.Utils;
import storm.starter.spout.RandomSentenceSpout;

import java.util.Map;
import java.util.Random;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopologyElastic {

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
      collector.emit(tuple,new Values(word, count));
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
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

    builder.setSpout("spout", new MyWordCount.WordGenerationSpout(), 1);

//    builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(Integer.parseInt(args[1])), 1).fieldsGrouping("spout", new Fields("word"));
    builder.setBolt("print", new Printer(),1).globalGrouping("count");

    Config conf = new Config();
    if(args.length>2&&args[2].equals("debug"))
      conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(1000000);

      cluster.shutdown();
    }
  }
}
