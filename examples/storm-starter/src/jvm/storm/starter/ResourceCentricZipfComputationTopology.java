package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricZipfComputationTopology {
    public static String StateMigrationCommandStream = "StateMigrationCommand";
    public static String StateMigrationStream = "StateMigration";
    public static String StateUpdateStream = "StateUpdate";
    public static String StateReadyStream = "SteateReady";
    public static String UpstreamCommand = "UpstreamCommand";

    public static String Spout = "spout";
    public static String GeneratorBolt = "generator";
    public static String ComputationBolt = "computation";
    public static String Controller = "controller";

    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name sleep-time-in-millis [debug|any other]");
        }

        TopologyBuilder builder = new TopologyBuilder();

        if(args.length < 3) {
            System.out.println("the number of args should be at least 1");
        }
        builder.setSpout(Spout, new ZipfSpout(), 1);

        builder.setBolt(GeneratorBolt, new ResourceCentricGeneratorBolt(),Integer.parseInt(args[1]))
                .allGrouping(Spout)
                .allGrouping(Controller, UpstreamCommand);


        builder.setBolt(ComputationBolt, new ResourceCentricComputationBolt(Integer.parseInt(args[2])), Integer.parseInt(args[3]))
                .directGrouping(GeneratorBolt)
                .directGrouping(GeneratorBolt, StateMigrationCommandStream)
                .directGrouping(Controller, StateUpdateStream);

        builder.setBolt(Controller, new ResourceCentricControllerBolt(), 1)
                .allGrouping(ComputationBolt, StateMigrationStream)
                .allGrouping(ComputationBolt, StateReadyStream);

        Config conf = new Config();
        //   if(args.length>2&&args[2].equals("debug"))
        //       conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
    }
}
