package storm.starter.util;

import backtype.storm.generated.MasterService;
import storm.starter.generated.ResourceCentricControllerService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by robert on 5/5/16.
 */
public class ShardReassignment {
    public static void main(String[] args) {

        System.out.println("command: sourceTaskId targetTaskId shardId");

        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,19090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            ResourceCentricControllerService.Client thriftClient = new ResourceCentricControllerService.Client(protocol);
            thriftClient.shardReassignment(Integer.getInteger(args[0]), Integer.getInteger(args[1]), Integer.getInteger(args[2]));
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
