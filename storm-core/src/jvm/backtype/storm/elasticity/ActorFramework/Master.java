package backtype.storm.elasticity.ActorFramework;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import backtype.storm.elasticity.ActorFramework.Message.HelloMessage;
import backtype.storm.elasticity.ActorFramework.Message.TaskMigrationCommandMessage;
import backtype.storm.generated.MasterService;
import backtype.storm.generated.MigrationException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    static Master _instance;

    public static Master getInstance() {
        return _instance;
    }

    public Master() {
        _instance = this;
        createThriftServiceThread();
    }

    public void createThriftServiceThread() {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                    System.out.println("Starting the monitoring daemon...");
                    server.serve();
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof HelloMessage) {
            HelloMessage helloMessage = (HelloMessage)message;
            if(_nameToActors.containsKey(helloMessage.getName()))
                System.out.println(helloMessage.getName()+" is registered again!");
            _nameToActors.put(helloMessage.getName(), getSender());
            System.out.format("[%s] is registered!\n",helloMessage.getName());
        } else if (message instanceof String) {
            System.out.println("Message received: " + message);
        }
    }

    public static Master createActor() {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Master.class), "master");
        return Master.getInstance();
    }

    public static void main(String[] args) {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Master.class), "master");

    }

    @Override
    public List<String> getAllHostNames() throws TException {
        return new ArrayList<>(_nameToActors.keySet());
    }

    @Override
    public void migrateTasks(String originalHostName, String targetHostName, int taskId, int routeNo) throws MigrationException, TException {
        if(!_nameToActors.containsKey(originalHostName))
            throw new MigrationException("originalHostName " + originalHostName + " does not exists!");
        if(!_nameToActors.containsKey(targetHostName))
            throw new MigrationException("targetHostName " + targetHostName + " does not exists!");
        _nameToActors.get(originalHostName).tell(new TaskMigrationCommandMessage(originalHostName,targetHostName,taskId,routeNo),getSelf());
        System.out.println("[Elastic]: Migration message has been set!");
    }


//    public class MasterService extends backtype.storm.generated.MasterService.Iface {
//
//    }
}
