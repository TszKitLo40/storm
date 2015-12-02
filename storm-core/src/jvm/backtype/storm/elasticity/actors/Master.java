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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    private Map<Integer, String> _taskidToActorName = new HashMap<>();

    private Map<String, String> _taskidRouteToHostName = new HashMap<>();

    private Map<String, String> _hostNameToWorkerLogicalName = new HashMap<>();

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

    private String getWorkerLogicalName(String hostName) {
        if(_hostNameToWorkerLogicalName.containsKey(hostName)) {
            return _hostNameToWorkerLogicalName.get(hostName);
        }
        else {
            return "Unknown worker name";
        }
    }

    private String getHostByWorkerLogicalName(String worker) {
        for(String key: _hostNameToWorkerLogicalName.keySet()) {
            if(_hostNameToWorkerLogicalName.get(key).equals(worker)) {
                return key;
            }
        }
            return "Unknown worker logical name";
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof HelloMessage) {
            HelloMessage helloMessage = (HelloMessage)message;
            if(_nameToActors.containsKey(helloMessage.getName()))
                System.out.println(helloMessage.getName()+" is registered again! ");
            _nameToActors.put(helloMessage.getName(), getSender());
            _hostNameToWorkerLogicalName.put(helloMessage.getName(), extractIpFromActorAddress(getSender().path().toString())+":"+helloMessage.getPort());
            System.out.format("[%s] is registered on %s!\n", helloMessage.getName(), _hostNameToWorkerLogicalName.get(helloMessage.getName()));
        } else if (message instanceof ElasticTaskRegistrationMessage) {
            ElasticTaskRegistrationMessage registrationMessage = (ElasticTaskRegistrationMessage) message;
            _taskidToActorName.put(registrationMessage.taskId, registrationMessage.hostName);
            System.out.println("Task " + registrationMessage.taskId + " is launched on " + getWorkerLogicalName(registrationMessage.hostName) +".");

        } else if (message instanceof RemoteRouteRegistrationMessage) {
            RemoteRouteRegistrationMessage registrationMessage = (RemoteRouteRegistrationMessage) message;
            for(int i: registrationMessage.routes) {
                if(!registrationMessage.unregister) {
                    _taskidRouteToHostName.put(registrationMessage.taskid + "." + i, registrationMessage.host);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + "is bound on " + getWorkerLogicalName(registrationMessage.host));
                } else {
                    _taskidRouteToHostName.remove(registrationMessage.taskid + "." + i);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + "is removed from " + getWorkerLogicalName(registrationMessage.host));
                }
            }

        } else if (message instanceof String) {
            System.out.println("message received: " + message);
        } else if (message instanceof LogMessage) {
            LogMessage logMessage = (LogMessage) message;
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            //get current date time with Date()
            Date date = new Date();
            System.out.println(dateFormat.format(date)+"[" + getWorkerLogicalName(logMessage.host) + "] "+ logMessage.msg);
        }
    }

    public static Master createActor() {
        try {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname="+ InetAddress.getLocalHost().getHostAddress()))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Master.class), "master");
        return Master.getInstance();
        }
        catch (UnknownHostException e ) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        try{
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname="+ InetAddress.getLocalHost().getHostAddress()))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Master.class), "master");
    }
    catch (UnknownHostException e ) {
        e.printStackTrace();
    }

    }

    @Override
    public List<String> getAllHostNames() throws TException {
        return new ArrayList<>(_nameToActors.keySet());
    }

    @Override
    public void migrateTasks(String originalHostName, String targetHostName, int taskId, int routeNo) throws MigrationException, TException {
        if(!_nameToActors.containsKey(getHostByWorkerLogicalName(originalHostName)))
            throw new MigrationException("originalHostName " + originalHostName + " does not exists!");
        if(!_nameToActors.containsKey(getHostByWorkerLogicalName(targetHostName)))
            throw new MigrationException("targetHostName " + targetHostName + " does not exists!");
        _nameToActors.get(getHostByWorkerLogicalName(originalHostName)).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName),getHostByWorkerLogicalName(targetHostName),taskId,routeNo),getSelf());
        System.out.println("[Elastic]: Migration message has been sent!");
    }

    @Override
    public void createRouting(String workerName, int taskid, int routeNo, String type) throws TException {
        if(!_nameToActors.containsKey(getHostByWorkerLogicalName(workerName))) {
            throw new HostNotExistException("Host " + workerName + " does not exist!");
        }

        _nameToActors.get(getHostByWorkerLogicalName(workerName)).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
        System.out.println("RoutingCreatingCommand has been sent!");
    }

    @Override
    public void withdrawRemoteRoute(String workerName, int taskid, int route) throws TException {
        if(!_taskidToActorName.containsKey(taskid)) {
            throw new TaskNotExistException("task "+ taskid + " does not exist!");
        }
        String hostName = _taskidToActorName.get(taskid);
        if(!_nameToActors.containsKey(hostName)) {
            throw new HostNotExistException("host " + hostName + " does not exist!");
        }
        RemoteRouteWithdrawCommand command = new RemoteRouteWithdrawCommand(getHostByWorkerLogicalName(workerName), taskid, route);
        _nameToActors.get(hostName).tell(command, getSelf());
        System.out.println("RemoteRouteWithdrawCommand has been sent to " + hostName);

    }

    @Override
    public double reportTaskThroughput(int taskid) throws TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(_nameToActors.get(_taskidToActorName.get(taskid)), new ThroughputQueryCommand(taskid));
        return (double)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));

    }

    @Override
    public String getDistribution(int taskid) throws TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(_nameToActors.get(_taskidToActorName.get(taskid)), new DistributionQueryCommand(taskid));
        return (String)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
    }

    String extractIpFromActorAddress(String address) {
        Pattern p = Pattern.compile( "@([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)" );
        Matcher m = p.matcher(address);
        if(m.find()) {
            return m.group(1);
        } else {
            System.err.println("cannot extract valid ip from " + address);
            return null;
        }
    }


//    public class MasterService extends backtype.storm.generated.MasterService.Iface {
//
//    }
}
