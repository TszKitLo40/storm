package backtype.storm.elasticity.actors;

import akka.actor.*;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.generated.HostNotExistException;
import backtype.storm.generated.MasterService;
import backtype.storm.generated.MigrationException;
import backtype.storm.generated.TaskNotExistException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    private Map<Integer, String> _taskidToActorName = new HashMap<>();

    private Map<String, String> _taskidRouteToHostName = new HashMap<>();

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
                System.out.println(helloMessage.getName()+" is registered again! ");
            _nameToActors.put(helloMessage.getName(), getSender());
            System.out.format("[%s] is registered!\n",helloMessage.getName());
        } else if (message instanceof ElasticTaskRegistrationMessage) {
            ElasticTaskRegistrationMessage registrationMessage = (ElasticTaskRegistrationMessage) message;
            _taskidToActorName.put(registrationMessage.taskId, registrationMessage.hostName);
            System.out.println("Task " + registrationMessage.taskId + " is launched on " + registrationMessage.hostName +".");

        } else if (message instanceof RemoteRouteRegistrationMessage) {
            RemoteRouteRegistrationMessage registrationMessage = (RemoteRouteRegistrationMessage) message;
            for(int i: registrationMessage.routes) {
                if(!registrationMessage.unregister) {
                    _taskidRouteToHostName.put(registrationMessage.taskid + "." + i, registrationMessage.host);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + "is bound on " + registrationMessage.host);
                } else {
                    _taskidRouteToHostName.remove(registrationMessage.taskid + "." + i);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + "is removed from " + registrationMessage.host);
                }
            }

        } else if (message instanceof String) {
            System.out.println("message received: " + message);
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
        _nameToActors.get(originalHostName).tell(new TaskMigrationCommand(originalHostName,targetHostName,taskId,routeNo),getSelf());
        System.out.println("[Elastic]: Migration message has been sent!");
    }

    @Override
    public void createRouting(String hostName, int taskid, int routeNo, String type) throws TException {
        if(!_nameToActors.containsKey(hostName)) {
            throw new HostNotExistException("Host " + hostName + " does not exist!");
        }

        _nameToActors.get(hostName).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
        System.out.println("RoutingCreatingCommand has been sent!");
    }

    @Override
    public void withdrawRemoteRoute(String remoteHostName, int taskid, int route) throws TException {
        if(!_taskidToActorName.containsKey(taskid)) {
            throw new TaskNotExistException("task "+ taskid + " does not exist!");
        }
        String hostName = _taskidToActorName.get(taskid);
        if(!_nameToActors.containsKey(hostName)) {
            throw new HostNotExistException("host " + hostName + " does not exist!");
        }
        RemoteRouteWithdrawCommand command = new RemoteRouteWithdrawCommand(remoteHostName, taskid, route);
        _nameToActors.get(hostName).tell(command, getSelf());
        System.out.println("RemoteRouteWithdrawCommand has been sent to " + hostName);

    }

    @Override
    public double reportTaskThroughput(int taskid) throws TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(_nameToActors.get(_taskidToActorName.get(taskid)), new ThroughputQueryCommand(taskid));
        return (double)inbox.receive(new FiniteDuration(1, TimeUnit.SECONDS));

    }


//    public class MasterService extends backtype.storm.generated.MasterService.Iface {
//
//    }
}
