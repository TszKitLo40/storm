package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.MyExclamation;
import storm.starter.MyWordCount;
import storm.starter.WordCountTopologyElastic;

/**
 * Created by acelzj on 14/04/16.
 */
public class ExclamationTopologyElastic {
    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name sleep-time-in-millis [debug|any other]");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 3);

        builder.setBolt("exaim1", new MyExclamation.ExclamationBolt(), 2).shuffleGrouping("word");
        builder.setBolt("exaim2", new MyExclamation.ExclamationBolt(),2).globalGrouping("exaim1");

        Config conf = new Config();
        if(args.length>2&&args[2].equals("debug"))
            conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

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
