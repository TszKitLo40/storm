package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
/**
 * Created by acelzj on 14/06/16.
 */
public class StreamEmitOrderTestTopology {

  public static final String FirstStream = "FirstStream";

  public static final String SecondStream = "SecondStream";

  public static class EmitSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;

    int tid;

    Thread emitThread;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      tid = 0;
//      emitThread = new Thread(new Runnable() {
//        @Override
//        public void run() {
//          while (true) {
//            while (tid <= 999) {
//              _collector.emit(FirstStream, new Values(String.valueOf(tid)));
//              tid += 1;
//            }
//            if (tid == 1000) {
//              _collector.emit(SecondStream, new Values(String.valueOf(tid)));
//              tid %= 1000;
//            }
//          }
//        }
//      });
//      emitThread.start();
    }

    @Override
    public void nextTuple() {
      if (tid <= 999) {
        _collector.emit(FirstStream, new Values(String.valueOf(tid)));
        tid += 1;
      } else if (tid == 1000) {
        _collector.emit(SecondStream, new Values(String.valueOf(tid)));
        tid %= 1000;
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream(FirstStream, new Fields("tid1"));
      declarer.declareStream(SecondStream, new Fields("tid2"));
    }
  }
 // public static class PrinterBolt extends BaseRichBolt {
    public static class PrinterBolt extends BaseElasticBolt {

    long _sleepInMillis;

    int preTID;

    int nextTID;

   // @Override
   /* public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    *  _collector = collector;
    *  _sleepInMillis = 1;
    *  preTID = -1;
    * } */

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
      Utils.sleep(_sleepInMillis);
      String streamId = tuple.getSourceStreamId();
      if (streamId.equals(FirstStream)) {
        nextTID = Integer.parseInt(tuple.getString(0));
//        System.out.println("TID is "+nextTID);
      //  if (preTID < 1000) {
        if (nextTID < preTID) {
          System.out.println("preTID is " + preTID);
          System.out.println("nextTID is " + nextTID);
          System.out.println("The order is wrong");
          preTID = nextTID;
        } else if (preTID == 1000 && nextTID != 0) {
          System.out.println("preTID is " + preTID);
          System.out.println("nextTID is " + nextTID);
          System.out.println("The order is wrong");
          preTID = nextTID;
          }
      } else if (streamId.equals(SecondStream)) {
        nextTID = Integer.parseInt(tuple.getString(0));
//        System.out.println("TID is "+nextTID);
        if (nextTID < preTID) {
          System.out.println("preTID is " + preTID);
          System.out.println("nextTID is " + nextTID);
          System.out.println("The order is wrong");
          preTID = nextTID;
        }
        }
    //    System.out.println(tuple.getInteger(0));
   //   } else if (streamId.equals(SecondStream)) {
   //         System.out.println(tuple.getInteger(0));
   //   }
//        collector.emit(tuple,new Values(nextTID));
//        collector.ack(tuple);
     // }
    }

   @Override
   public Object getKey(Tuple tuple) {
     return tuple.getString(0);
   }

   @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

   @Override
   public void prepare(Map stormConf, TopologyContext context) {
     _sleepInMillis = 1;
     preTID = -1;
     declareStatefulOperator();
   }
 }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new EmitSpout(), 1);

    builder.setBolt("printer", new PrinterBolt(), 1)
        .allGrouping("spout", FirstStream)
        .allGrouping("spout", SecondStream);

    Config conf = new Config();
    //   if(args.length>2&&args[2].equals("debug"))
    //       conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(8);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
  }
}

