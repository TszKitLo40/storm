package storm.starter.util;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import storm.starter.generated.ResourceCentricControllerService;

/**
 * Created by robert on 16-7-25.
 */
public class QueryRoutingTable {
    public static void main(String[] args) {

        System.out.println("command: thrift_ip");


        try {
            TTransport transport = new TSocket(args[0],19090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            ResourceCentricControllerService.Client thriftClient = new ResourceCentricControllerService.Client(protocol);
//            int sum = Integer.getInteger(args[0]);
//            sum += Integer.getInteger(args[1]);
//            sum += Integer.getInteger(args[2]);
//            System.out.println(sum);

            System.out.println(thriftClient.queryRoutingTable());
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
