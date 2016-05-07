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

        System.out.println("command: thrift-ip sourceTaskId targetTaskId shardId");


        try {
            TTransport transport = new TSocket(args[0],19090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            ResourceCentricControllerService.Client thriftClient = new ResourceCentricControllerService.Client(protocol);
//            int sum = Integer.getInteger(args[0]);
//            sum += Integer.getInteger(args[1]);
//            sum += Integer.getInteger(args[2]);
//            System.out.println(sum);

            thriftClient.shardReassignment(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
