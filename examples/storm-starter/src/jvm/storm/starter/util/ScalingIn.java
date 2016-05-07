package storm.starter.util;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import storm.starter.generated.ResourceCentricControllerService;

/**
 * Created by robert on 5/5/16.
 */
public class ScalingIn {
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

            thriftClient.scalingIn();
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
