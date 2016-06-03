package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.FileWriter;

/**
 * Created by Robert on 11/20/15.
 */
public class QueryAllThroughput {

    public static void main(String[] args) throws IOException{

        if(args.length<2) {
            System.out.println("args: taskid, cycle-in-secs");
            return;
        }
     //   File file = new File("/home/acelzj/storm/storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/throughput/ThroughtWithoutLoadBalancingAndAutoScaling.txt");
        File file = new File("/home/acelzj/storm/storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/throughput/ThroughtWithLoadBalancingAndAutoScaling.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fop = new FileOutputStream(file);
        int length = args.length;
        List<Integer> tasks = new ArrayList<Integer>();
        for(int i = 0; i < length-1; ++i){
            tasks.add(Integer.parseInt(args[i]));
        }
     //   int taskid = Integer.parseInt(args[0]);
        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            MasterService.Client thriftClient = new MasterService.Client(protocol);

            int i = 0;
            while(true) {
                Thread.sleep(1000*Integer.parseInt(args[length-1]));
                try{
                    double throughput = 0;
                for(int taskid : tasks) {
                    throughput += thriftClient.reportTaskThroughput(taskid);
                    //     String content = "Task "+ taskid + ": "+throughput;
                    //        String newline = System.getProperty("line.separator");
                    //    byte[] contentInBytes = content.getBytes();
                    //     byte[] nextlineInBytes = newline.getBytes();
                    //     fop.write(contentInBytes);
                    //     fop.write(nextlineInBytes);
                    System.out.println("Task " + taskid + ": " + throughput);
                }
                    String content = ""+throughput;
                    String newline = System.getProperty("line.separator");
                    byte[] contentInBytes = content.getBytes();
                    byte[] nextlineInBytes = newline.getBytes();
                    fop.write(contentInBytes);
                    fop.write(nextlineInBytes);
                    System.out.println("Throughput of all the tasks is" + ": " + throughput);

             /*       if (i%5==0) {
                        System.out.println(thriftClient.queryDistribution(taskid));
                       // fw.write(thriftClient.queryDistribution(taskid));
                       // fw.write(nextlineInBytes);
                    }*/

                } catch (TException e ) {
                    e.printStackTrace();
                }
                i++;
            }

//            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        fop.flush();
 //       fop.close();
        System.out.println("finished!");
    }
}
