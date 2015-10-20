package storm.starter;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorMigration;
import backtype.storm.generated.ExecutorMigrationOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Created by robert on 10/19/15.
 */
public class Migrate {
    public static void main(String[] args){
        Map conf = Utils.readStormConfig();

        NimbusClient Nimbusclient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client client = Nimbusclient.getClient();

        ExecutorMigrationOptions options = new ExecutorMigrationOptions();
        ExecutorMigration executorMigration = new ExecutorMigration();
        executorMigration.set_desc_ip("desc_ip");
        executorMigration.set_desc_port("desc_port");
        ExecutorInfo executorInfo = new ExecutorInfo();
        executorInfo.set_task_start(10);
        executorInfo.set_task_end(11);
        executorMigration.set_executor(executorInfo);
        List < ExecutorMigration > executorMigrations = new Vector<>();
        executorMigrations.add(executorMigration);
        options.set_migrations(executorMigrations);
        try {
            client.migrateExecutor(args[0], options);
            System.out.println("submitted!");
        } catch (Exception e ) {
            e.printStackTrace();
        }
    }
}
