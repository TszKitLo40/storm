package backtype.storm.elasticity.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import backtype.storm.elasticity.actors.utils.ScalingInSubtask;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.message.actormessage.Status;
import backtype.storm.elasticity.resource.ResourceManager;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.generated.HostNotExistException;
import backtype.storm.generated.MasterService;
import backtype.storm.generated.MigrationException;
import backtype.storm.generated.TaskNotExistException;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.omg.CORBA.TIMEOUT;
import scala.Array;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    Cluster cluster = Cluster.get(getContext().system());

//    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    private Map<String, ActorPath> _nameToPath = new ConcurrentHashMap<>();

    private Map<Integer, String> _taskidToActorName = new ConcurrentHashMap<>();

    public Map<Integer, String> _elasticTaskIdToWorkerLogicalName = new HashMap<>();

    public Map<String, String> _taskidRouteToWorker = new ConcurrentHashMap<>();

    private Map<String, String> _hostNameToWorkerLogicalName = new HashMap<>();

    private Map<String, Set<String>> _ipToWorkerLogicalName = new HashMap<>();

    private Set<String> _supervisorActorNames = new HashSet<>();

    private Map<Integer, List<String>> _taskToCoreLocations = new HashMap<>();

//    IConnection _loggingInput;

    private Inbox _inbox; // use a single inbox to avoid a bug that occasionally causes message delivery failures.

    static Master _instance;

    public static Master getInstance() {
        return _instance;
    }

    public Master() {
        _instance = this;
        createThriftServiceThread();
    }

    private Inbox getInbox() {
        if(_inbox == null) {
            _inbox = Inbox.create(getContext().system());
        }
        return _inbox;
    }

    private synchronized Object sendAndReceive(ActorRef ref, Object message) throws TimeoutException {
        Inbox inbox = getInbox();
        inbox.send(ref, message);
        return inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));
    }

//    public void deployLoggingServer() {
//        _loggingInput = new MyContext().bind("logging", backtype.storm.elasticity.config.Config.LoggingServerPort);
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while(true) {
//                    Iterator<TaskMessage> iterator = _loggingInput.recv(0, 0);
//                    while(iterator.hasNext()) {
//                        TaskMessage message = iterator.next();
//
//                    }
//                }
//            }
//        })
//    }

    public void createThriftServiceThread() {

//        System.out.println("thrift Service Thread is called!");
//        System.out.println(Thread.currentThread().getStackTrace());
//        Thread.dumpStack();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    log("Begin to create thrift daemon thread...");
                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                    log("Starting the monitoring daemon...");
                    server.serve();
                } catch (Exception e) {
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

//    @Override
//    public void preStart() {
////        cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class);
//    }
//
//    @Override
//    public void postStop() {
//        cluster.unsubscribe(getSelf());
//    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.UnreachableMember.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof ClusterEvent.UnreachableMember) {
            ClusterEvent.UnreachableMember unreachableMember = (ClusterEvent.UnreachableMember) message;
            log(unreachableMember.member().address().toString() + " is unreachable!");

            if(_supervisorActorNames.contains(unreachableMember.member().address().toString())) {
                final String ip = extractIpFromActorAddress(unreachableMember.member().address().toString());
                System.out.println(String.format("Supervisor on %s is dead!", ip));
                _supervisorActorNames.remove(unreachableMember.member().address().toString());
                ResourceManager.instance().computationResource.unregisterNode(ip);
                System.out.println("Resource Manager is updated upon the dead of the supervisor.");
                return;
            }

            for(String name: _nameToPath.keySet()) {
                if(_nameToPath.get(name).address().toString().equals(unreachableMember.member().address().toString())){
                    final String ip = extractIpFromActorAddress(unreachableMember.member().address().toString());
                    if(ip == null) {
                        continue;
                    }
                    final String logicalName = _hostNameToWorkerLogicalName.get(name);

                    _ipToWorkerLogicalName.get(ip).remove(logicalName);
//                    if(_ipToWorkerLogicalName.get(ip).isEmpty()) {
//                        _ipToWorkerLogicalName.remove(ip);
////                        ResourceManager.instance().computationResource.unregisterNode(ip);
//                    }
                    for(int task: _taskidToActorName.keySet()) {

                        if(_nameToPath.get(_taskidToActorName.get(task)).address().toString().equals(unreachableMember.member().address().toString())) {
                            System.out.println("_taskidToActorName--> Task: " + task + " + _taskidToActorName: " + _taskidToActorName.get(task));
                            int i = 1;
                            if(_taskToCoreLocations.get(task)!=null) {
                                for (String coreIp : _taskToCoreLocations.get(task)) {
                                    System.out.println("No: " + i++);
                                    ResourceManager.instance().computationResource.returnProcessor(coreIp);
                                    System.out.println("The CPU core for task " + task + " is returned, as the its hosting worker is called!");
                                }
                            }

                            _elasticTaskIdToWorkerLogicalName.remove(task);
                            _taskidToActorName.remove(task);
                            _taskToCoreLocations.remove(task);
                            for(String str: _taskidRouteToWorker.keySet()) {
                                if(str.split("\\.")[0].equals(""+task)) {
                                    _taskidRouteToWorker.remove(str);
                                }
                            }
                        }
                    }

                    _nameToPath.remove(name);
                    log(_hostNameToWorkerLogicalName.get(name)+" is removed from the system.");
                    _hostNameToWorkerLogicalName.remove(name);


                } else {
//                    System.out.println(_nameToPath.get(name) + " != " + unreachableMember.member().address().toString());
                }

            }
//
//            System.out.println("_nameToPath: "+_nameToPath);
//            System.out.println("_nameToWorkerLogicalName: " + _hostNameToWorkerLogicalName);

        } else if(message instanceof SupervisorRegistrationMessage){
            SupervisorRegistrationMessage supervisorRegistrationMessage = (SupervisorRegistrationMessage)message;
            final String ip = extractIpFromActorAddress(getSender().path().toString());
            ResourceManager.instance().computationResource.registerNode(ip, supervisorRegistrationMessage.getNumberOfProcessors());
            _supervisorActorNames.add(getSender().path().address().toString());
            System.out.println(String.format("Supervisor is registered on %s with %d processors", ip, supervisorRegistrationMessage.getNumberOfProcessors()));

        } else if(message instanceof WorkerRegistrationMessage) {
            WorkerRegistrationMessage workerRegistrationMessage = (WorkerRegistrationMessage)message;
            if(_nameToPath.containsKey(workerRegistrationMessage.getName()))
                log(workerRegistrationMessage.getName()+" is registered again! ");
//            _nameToActors.put(workerRegistrationMessage.getName(), getSender());
            _nameToPath.put(workerRegistrationMessage.getName(), getSender().path());
            final String ip = extractIpFromActorAddress(getSender().path().toString());
            if(ip == null) {
                System.err.println("WorkerRegistrationMessage is ignored, as we cannot extract a valid ip!");
                return;
            }
            final String logicalName = ip +":"+ workerRegistrationMessage.getPort();
            _hostNameToWorkerLogicalName.put(workerRegistrationMessage.getName(), logicalName);

            if(!_ipToWorkerLogicalName.containsKey(ip))
                _ipToWorkerLogicalName.put(ip, new HashSet<String>());
            _ipToWorkerLogicalName.get(ip).add(logicalName);
//            ResourceManager.instance().computationResource.registerNode(ip, workerRegistrationMessage.getNumberOfProcessors());
            System.out.println(ResourceManager.instance().computationResource);

            log("[" + workerRegistrationMessage.getName() + "] is registered on " + _hostNameToWorkerLogicalName.get(workerRegistrationMessage.getName()));
            getSender().tell(new WorkerRegistrationResponseMessage(InetAddress.getLocalHost().getHostAddress(), ip, workerRegistrationMessage.getPort()), getSelf());
        } else if (message instanceof ElasticTaskRegistrationMessage) {
            ElasticTaskRegistrationMessage registrationMessage = (ElasticTaskRegistrationMessage) message;
            _taskidToActorName.put(registrationMessage.taskId, registrationMessage.hostName);
            _elasticTaskIdToWorkerLogicalName.put(registrationMessage.taskId, getWorkerLogicalName(registrationMessage.hostName));
            log("Task " + registrationMessage.taskId + " is launched on " + getWorkerLogicalName(registrationMessage.hostName) + ".");

        } else if (message instanceof RouteRegistrationMessage) {
            RouteRegistrationMessage registrationMessage = (RouteRegistrationMessage) message;
            for(int i: registrationMessage.routes) {
                if(!registrationMessage.unregister) {
                    _taskidRouteToWorker.put(registrationMessage.taskid + "." + i, getWorkerLogicalName(registrationMessage.host));
                    System.out.println("Route " + registrationMessage.taskid + "." + i + " is bound on " + getWorkerLogicalName(registrationMessage.host));
                } else {
                    _taskidRouteToWorker.remove(registrationMessage.taskid + "." + i);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + " is removed from " + getWorkerLogicalName(registrationMessage.host));
                }
//                for(String n: _taskidRouteToWorker.keySet()) {
//                    System.out.println(String.format("%s: %s", n, _taskidRouteToWorker.get(n)));
//                }
            }

        } else if (message instanceof String) {
            log("message received: " + message);
        } else if (message instanceof LogMessage) {
            LogMessage logMessage = (LogMessage) message;
            //get current date time with Date()
            log(getWorkerLogicalName(logMessage.host), logMessage.msg);
//            System.out.println(dateFormat.format(date)+"[" + getWorkerLogicalName(logMessage.host) + "] "+ logMessage.msg);
        } else if (message instanceof WorkerCPULoad) {
            WorkerCPULoad load = (WorkerCPULoad) message;
            ResourceManager.instance().updateWorkerCPULoad(getWorkerLogicalName(load.hostName), load);
        } else if (message instanceof ExecutorScalingInRequestMessage) {
            ExecutorScalingInRequestMessage requestMessage = (ExecutorScalingInRequestMessage)message;
            ElasticScheduler.getInstance().addScalingRequest(requestMessage);
//            handleExecutorScalingInRequest(requestMessage.taskID);
        } else if (message instanceof ExecutorScalingOutRequestMessage) {
            ExecutorScalingOutRequestMessage requestMessage = (ExecutorScalingOutRequestMessage) message;
//            handleExecutorScalingOutRequest(requestMessage.taskId);
            ElasticScheduler.getInstance().addScalingRequest(requestMessage);
        }
    }

    public void handleExecutorScalingInRequest(int taskId) {
        try {
            RoutingTable routingTable = getRoutingTable(taskId);
            int targetRouteId = routingTable.getNumberOfRoutes() - 1;
            String hostWorkerLogicalName = _taskidRouteToWorker.get(taskId+"."+targetRouteId);

            if(hostWorkerLogicalName==null) {
                System.out.println("hostWorkerLogicalName is null!");
                System.out.println(taskId+"."+targetRouteId);
                for(String n: _taskidRouteToWorker.keySet()) {
                    System.out.println(String.format("%s: %s", n, _taskidRouteToWorker.get(n)));
                }
            }


            String hostIp = getIpForWorkerLogicalName(hostWorkerLogicalName);
            System.out.println("ScalingInSubtask will be called!");
            if(scalingInSubtask(taskId)) {
                System.out.println("Task" + taskId + " successfully scales in!");
                ResourceManager.instance().computationResource.returnProcessor(hostIp);
            } else {
                System.out.println("Task " + taskId + " scaling in fails!");
            }
//            System.out.println("Current Routing Table: ");
//            System.out.println(getOriginalRoutingTable(taskId));
//            System.out.println("=====================================\n");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    String getIpForWorkerLogicalName(String workerLogicalName) {
        String hostIp = null;
        for(String ip : _ipToWorkerLogicalName.keySet()) {
            if(_ipToWorkerLogicalName.get(ip).contains(workerLogicalName)) {
                hostIp = ip;
                break;
            }
        }
        if(hostIp == null) {
            System.out.println(String.format("Cannot find ip for %s", workerLogicalName));
            System.out.println("_ipToWorkerLogicalName:");
            for(String ip: _ipToWorkerLogicalName.keySet()) {
                System.out.println(String.format("%s : %s", ip, _ipToWorkerLogicalName.get(ip)));
            }
        }
        return hostIp;

    }

    public void handleExecutorScalingOutRequest(int taskid) {

        try {
            String workerHostName = _elasticTaskIdToWorkerLogicalName.get(taskid);
            String preferredIp = getIpForWorkerLogicalName(workerHostName);

            String hostIp = ResourceManager.instance().computationResource.allocateProcessOnPreferredNode(preferredIp);

            if(hostIp == null) {
                System.err.println("There is not enough computation resources for scaling out!");
                return;
            }

            if(!_taskToCoreLocations.containsKey(taskid)) {
                _taskToCoreLocations.put(taskid, new ArrayList<String>());
            }
            _taskToCoreLocations.get(taskid).add(hostIp);

            RoutingTable balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
            if(balancecHashRouting == null) {
                createRouting(workerHostName,taskid,1,"balanced_hash");
                balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
            }

            System.out.println("ScalingOutSubtask will be called!");
            scalingOutSubtask(taskid);
            System.out.println("ScalingOutSubtask is called!");
//            System.out.println("A local new task " + taskid + "." + balancecHashRouting.getNumberOfRoutes() +" is created!");
//            for(String name: _ipToWorkerLogicalName.keySet()) {
//                System.out.println(String.format("%s: %s", name, _ipToWorkerLogicalName.get(name)));
//            }

            if(!hostIp.equals(preferredIp)) {
                Set<String> candidateHosterWorkers = _ipToWorkerLogicalName.get(hostIp);

                List<String> candidates = new ArrayList<>();
                candidates.addAll(candidateHosterWorkers);
                String hosterWorker = candidates.get(new Random().nextInt(candidateHosterWorkers.size()));

                int targetRoute = getRoutingTable(taskid).getNumberOfRoutes() - 1 ;

                System.out.println("a local task " + taskid + "." + targetRoute+" will be migrated from " + workerHostName + " to " + hosterWorker);
                migrateTasks(workerHostName, hosterWorker, taskid, targetRoute);
                System.out.println("Task " + taskid + "." + targetRoute + " has been migrated!");
            }

//            System.out.println("Current Routing Table: ");
//            System.out.println(getOriginalRoutingTable(taskid));
//            System.out.println("=====================================\n");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    void log(String logger, String content) {
        DateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();
        System.out.println(dateFormat.format(date)+ " [" + logger + "]: " + content);
    }

    void log(String content) {
        log("Master", content);
    }

    public static Master createActor() {
            final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                    .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + backtype.storm.elasticity.config.Config.masterIp))
                    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                    .withFallback(ConfigFactory.load()); 
            ActorSystem system = ActorSystem.create("ClusterSystem", config);

            system.actorOf(Props.create(Master.class), "master");
            Master ret = null;
            while(ret==null) {
                ret = Master.getInstance();
                Utils.sleep(100);
                System.out.println("Waiting Elastic Master to launch!");
            }
            return Master.getInstance();


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
//        return new ArrayList<>(_nameToActors.keySet());
        return new ArrayList<>(_nameToPath.keySet());
    }

    @Override
    public void migrateTasks(String originalHostName, String targetHostName, int taskId, int routeNo) throws MigrationException, TException {
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(originalHostName)))
            throw new MigrationException("originalHostName " + originalHostName + " does not exists!");
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(targetHostName)))
            throw new MigrationException("targetHostName " + targetHostName + " does not exists!");
        try {
//            getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskId))).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName), getHostByWorkerLogicalName(targetHostName), taskId, routeNo), getSelf());
//            log("[Elastic]: Migration message has been sent!");

//            final Inbox inbox = Inbox.create(getContext().system());
            final Inbox inbox = getInbox();
//            inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskId))), new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName), getHostByWorkerLogicalName(targetHostName), taskId, routeNo));
//            inbox.receive(new FiniteDuration(2000, TimeUnit.SECONDS));


            return;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createRouting(String workerName, int taskid, int routeNo, String type) throws TException {
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(workerName))) {
            throw new HostNotExistException("Host " + workerName + " does not exist!");
        }
        try {
//            final Inbox inbox = Inbox.create(getContext().system());
            final Inbox inbox = getInbox();
            inbox.send(getContext().actorFor(_nameToPath.get(getHostByWorkerLogicalName(workerName))), new RoutingCreatingCommand(taskid, routeNo, type));
//            getContext().actorFor(_nameToPath.get(getHostByWorkerLogicalName(workerName))).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
//          _nameToActors.get(getHostByWorkerLogicalName(workerName)).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
            inbox.receive(new FiniteDuration(10000, TimeUnit.SECONDS));
            log("RoutingCreatingCommand has been sent!");
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void withdrawRemoteRoute(int taskid, int route) throws TException {
        if(!_taskidToActorName.containsKey(taskid)) {
            throw new TaskNotExistException("task "+ taskid + " does not exist!");
        }
        String hostName = _taskidToActorName.get(taskid);
        if(!_nameToPath.containsKey(hostName)) {
            throw new HostNotExistException("host " + hostName + " does not exist!");
        }
        RemoteRouteWithdrawCommand command = new RemoteRouteWithdrawCommand(getHostByWorkerLogicalName(hostName), taskid, route);
        try {
            getContext().actorFor(_nameToPath.get(hostName)).tell(command, getSelf());
            log("RemoteRouteWithdrawCommand has been sent to " + hostName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

//        _nameToActors.get(hostName).tell(command, getSelf());
//        System.out.println("RemoteRouteWithdrawCommand has been sent to " + hostName);

    }

    @Override
    public double reportTaskThroughput(int taskid) throws TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
        final Inbox inbox = getInbox();
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ThroughputQueryCommand(taskid));
//        inbox.send(_nameToActors.get(_taskidToActorName.get(taskid)), new ThroughputQueryCommand(taskid));
        try {
        return (double)inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return -1;
        }

    }

    @Override
    public String queryDistribution(int taskid) throws TException {
        return getDistributionHistogram(taskid).toString();
    }


    @Override
    public String getLiveWorkers() throws TException {
        String ret = "";
        for(String name: _nameToPath.keySet()) {
            ret +=_hostNameToWorkerLogicalName.get(name) + ":" + name +"\n";
        }
        ret += "_hostNameToWorkerLogicalName" + _hostNameToWorkerLogicalName;
        return ret;
    }

    @Override
    public String queryRoutingTable(int taskid) throws TaskNotExistException, TException {
//        if(!_taskidToActorName.containsKey(taskid))
//            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
//        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
//        return (String)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
        return getRoutingTable(taskid).toString();
    }

    @Override
    public void reassignBucketToRoute(int taskid, int bucket, int originalRoute, int newRoute) throws TaskNotExistException, TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        ReassignBucketToRouteCommand command = new ReassignBucketToRouteCommand(taskid, bucket, originalRoute, newRoute);
//        final Inbox inbox = Inbox.create(getContext().system());
        final Inbox inbox = getInbox();
//        System.out.println("\n======================= BEGIN SHARD REASSIGNMENT =======================");


        long startTime = System.currentTimeMillis();

        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), command);
        try {
            inbox.receive(new FiniteDuration(2000, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return;
        }

//        System.out.println("Shard reassignment time: " + (System.currentTimeMillis() - startTime));

//        System.out.println("======================= End SHARD REASSIGNMENT =======================\n");
//        getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))).tell(command, getSelf());
    }

    @Override
    public String optimizeBucketToRoute(int taskid) throws TaskNotExistException, TException {
        try {
            return ElasticScheduler.getInstance().optimizeBucketToRoutingMapping(taskid);
        } catch (RoutingTypeNotSupportedException e) {
            e.printStackTrace();
            return e.getMessage();
        } catch (Exception ee) {
            ee.printStackTrace();
            return ee.getMessage();
        }
    }

    @Override
    public String optimizeBucketToRouteWithThreshold(int taskid, double theshold) throws TaskNotExistException, TException {
        try {
           return ElasticScheduler.getInstance().optimizeBucketToRoutingMapping(taskid, theshold);
        } catch (RoutingTypeNotSupportedException e) {
            e.printStackTrace();
            return e.getMessage();
        } catch (Exception ee) {
            ee.printStackTrace();
            return ee.getMessage();
        }
    }

    @Override
    public String subtaskLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
//        final Inbox inbox = Inbox.create(getContext().system());
//        final Inbox inbox = getInbox();
//        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new SubtaskLevelLoadBalancingCommand(taskid));
        try {
        //    return (String)inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));
            return (String)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new SubtaskLevelLoadBalancingCommand(taskid));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return "Timeout!";
        }
    }

    @Override
    public String workerLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
        try {
            return ElasticScheduler.getInstance().workerLevelLoadBalancing(taskid);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @Override
    public String queryWorkerLoad() throws TException {
        return ResourceManager.instance().printString();
    }

    @Override
    public String naiveWorkerLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid))
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        return ElasticScheduler.getInstance().naiveWorkerLevelLoadBalancing(taskid);
    }

    @Override
    public void scalingOutSubtask(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
//        final Inbox inbox = Inbox.create(getContext().system());
        final Inbox inbox = getInbox();
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingOutSubtaskCommand(taskid));
        System.out.println("Scaling out message has been sent!");
        try {
            inbox.receive(new FiniteDuration(30000, TimeUnit.SECONDS));

        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        System.out.println("Scaling out response is received!");
    }

    @Override
    public boolean scalingInSubtask(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        final Inbox inbox = getInbox();
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingInSubtaskCommand(taskid));
        System.out.println("Scaling in message has been sent!");
        try {
            Status returnStatus = (Status)inbox.receive(new FiniteDuration(30000, TimeUnit.SECONDS));
            System.out.println("Scaling in response is received!");
//            System.out.println("Sender: " + getSender().toString());
//        System.out.println(returnStatus);
        return returnStatus.code == Status.OK;
        } catch (TimeoutException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void logOnMaster(String from, String msg) throws TException {
        log(from, msg);
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

    public Histograms getDistributionHistogram(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
  //      final Inbox inbox = getInbox();
 //       inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new DistributionQueryCommand(taskid));
        try {
 //           return (Histograms)inbox.receive(new FiniteDuration(10, TimeUnit.SECONDS));
            return (Histograms)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new DistributionQueryCommand(taskid));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public RoutingTable getRoutingTable(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
//        final Inbox inbox = getInbox();
 //       inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
        try {
 //           return (RoutingTable)inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));

            return (RoutingTable)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public RoutingTable getOriginalRoutingTable(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
        final Inbox inbox = getInbox();
        RoutingTableQueryCommand command = new RoutingTableQueryCommand(taskid);
        command.completeRouting = false;
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), command);
        try {
            return (RoutingTable)inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));

        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Histograms getBucketDistribution(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
 //       final Inbox inbox = getInbox();
//        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new BucketDistributionQueryCommand(taskid));
        try {
//            return (Histograms)inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));
              return (Histograms)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new BucketDistributionQueryCommand(taskid));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getRouteHosterName(int taskid, int route) {
        return _taskidRouteToWorker.get(taskid + "." + route);
    }

}
