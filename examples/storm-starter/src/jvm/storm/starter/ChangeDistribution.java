package storm.starter;

import backtype.storm.generated.MasterService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by acelzj on 04/05/16.
 */
public class ChangeDistribution {
    public static void main(String[] args) {

        if(args.length!=2) {
            System.out.println("args: numberOfElements exponent ");
            return;
        }

        TTransport transport = new TSocket("192.168.0.120",9080);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            ChangeDistributionService.Client thriftClient = new ChangeDistributionService.Client(protocol);
            thriftClient.changeNumberOfElements(Integer.parseInt(args[0]));
            thriftClient.changeExponent(Double.parseDouble(args[1]));
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("finished!");
    }
}
