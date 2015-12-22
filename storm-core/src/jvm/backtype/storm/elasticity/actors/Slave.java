package backtype.storm.elasticity.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import backtype.storm.elasticity.ElasticTaskHolder;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.utils.Histograms;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Slave extends UntypedActor {

    Cluster cluster = Cluster.get(getContext().system());

//    Map<String, ActorRef> _nameToActors = new HashMap<>();

    Map<String, ActorPath> _nameToPath = new ConcurrentHashMap<>();

    String _name;

    ActorSelection _master;

    static Slave _instance;

    int _port;

    public Slave(String name, String port) {
//        _name = name+":"+port+"-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        _name = name + ":" + port;
        _port = Integer.parseInt(port);
        _instance = this;
        System.out.println("Slave constructor is called!");
//        createLiveNotificationHeartbeat();
    }

    private void createLiveNotificationHeartbeat() {
        final int signiture = new Random().nextInt();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(10000);
                        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
//                        ;
                        sendMessageToMaster("I am still alive! " + signiture + "Memory usage: " + heapMemoryUsage.getUsed()/1024/1024 + "MB!");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static Slave getInstance() {
        return _instance;
    }

    public static Slave waitAndGetInstance() {
        try{
            while(_instance==null) {
                Thread.sleep(1000);
            }
            return _instance;
        } catch (InterruptedException e) {
            return _instance;
        }
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class, ClusterEvent.UnreachableMember.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof ClusterEvent.CurrentClusterState) {
            ClusterEvent.CurrentClusterState state = (ClusterEvent.CurrentClusterState) message;
            for (Member member : state.getMembers()) {
                if (member.status().equals(MemberStatus.up())) {
                    register(member);
//                    member.address().toString();
                }
            }
        } else if (message instanceof ClusterEvent.MemberUp) {
            ClusterEvent.MemberUp memberUp = (ClusterEvent.MemberUp) message;
            register(memberUp.member());
        } else if (message instanceof ClusterEvent.UnreachableMember) {
            ClusterEvent.UnreachableMember unreachableMember = (ClusterEvent.UnreachableMember) message;
            System.out.println(unreachableMember.member().address().toString() + " is unreachable!");

            for(String name: _nameToPath.keySet()) {
                if(_nameToPath.get(name).address().toString().equals(unreachableMember.member().address().toString())){
                    System.out.println(name+" is removed! "+ _nameToPath.get(name).address().toString() +"-->"+unreachableMember.member().address().toString());
                    _nameToPath.remove(name);
                    sendMessageToMaster(name+" is removed! ");
                } else {
                    System.out.println(_nameToPath.get(name).address().toString() + " != " + unreachableMember.member().address().toString());
                }

            }
        }else if (message instanceof HelloMessage) {
            HelloMessage helloMessage = (HelloMessage) message;
//            _nameToActors.put(helloMessage.getName(), getSender());
            _nameToPath.put(helloMessage.getName(), getSender().path());
//            getContext().actorFor( getSender().path()).tell("Hi, I am " + _name + " Your path is "+ getSender().path().toString(), getSelf() );
            System.out.println("[Elastic]: I am connected with " + ((HelloMessage) message).getName() + "[" + getSender() + "]");
//            sendMessageToMaster("I am connected with " + helloMessage.getName());
        } else if (message instanceof TaskMigrationCommand) {
            System.out.println("[Elastic]: recieved  TaskMigrationCommand!");
            TaskMigrationCommand taskMigrationCommand = (TaskMigrationCommand) message;
            handleTaskMigrationCommandMessage(taskMigrationCommand);
//            if(!_nameToActors.containsKey(taskMigrationCommand._targetHostName)) {
//                System.out.println("[Elastic]:target host "+taskMigrationCommand._targetHostName+"does not exist!");
//                return;
//            }
//
//            ElasticTaskMigrationMessage migrationMessage = ElasticTaskHolder.instance().generateRemoteElasticTasks(taskMigrationCommand._taskID, taskMigrationCommand._route);
//            if(migrationMessage!=null) {
//                System.out.print("The number of routes in the generated elastic tasks:"+migrationMessage._elasticTask.get_routingTable().getRoutes().size());
//
//                _nameToActors.get(taskMigrationCommand._targetHostName).tell(migrationMessage, getSelf());
//                System.out.println("[Elastic]: elastic message has been sent to "+_nameToActors.get(taskMigrationCommand._targetHostName)+"["+_nameToActors.get(taskMigrationCommand._targetHostName).path()+"]");
//                _master.tell("I have passed the elastic message to "+taskMigrationCommand._targetHostName,null);
//            } else {
//                _master.tell("I do not contains the task for task id"+taskMigrationCommand._taskID,null);
//            }

        } else if (message instanceof ElasticTaskMigrationMessage) {
            sendMessageToMaster("Received Migration Message!!!!");
            handleElasticTaskMigrationMessage((ElasticTaskMigrationMessage) message);
//            System.out.println("[Elastic]: received elastic mask migration message from"+getSender());
//            _master.tell("I received elastic mask migration message from "+getSender().path(),getSelf());
//            ElasticTaskMigrationConfirmMessage confirmMessage = ElasticTaskHolder.instance().handleGuestElasticTasks(addIpInfo((ElasticTaskMigrationMessage)message,getSender().path().toString()));
//            _master.tell("I generate confirm message ",getSelf());
//
//            if(confirmMessage!=null) {
//                getSender().tell(confirmMessage, getSelf());
//                _master.tell("I have handled the mask migration message",getSelf());
//            } else {
//                System.err.println("Failed to deploy remote elastic tasks!");
//                _master.tell("Failed to deploy elastic tasks", null);
//            }
        } else if (message instanceof ElasticTaskMigrationConfirmMessage) {
            ElasticTaskMigrationConfirmMessage confirmMessage = (ElasticTaskMigrationConfirmMessage) message;
            handleElasticTaskMigrationConfirmMessage(confirmMessage);
//            confirmMessage._ip=extractIpFromActorAddress(getSender().path().toString());
//
//            String ip = confirmMessage._ip;
//            int port = confirmMessage._port;
//            int taskId = confirmMessage._taskId;
//
//            ElasticTaskHolder holder = ElasticTaskHolder.instance();
//            System.out.print("Received ElasticTaskMigrationConfirmMessage #. routes: "+confirmMessage._routes.size());
//            for(int i: confirmMessage._routes) {
//                holder.establishConnectionToRemoteTaskHolder(taskId, i, ip, port);
//            }

        } else if (message instanceof RoutingCreatingCommand) {
            RoutingCreatingCommand creatingCommand = (RoutingCreatingCommand) message;
            handleRoutingCreatingCommand(creatingCommand);

        } else if (message instanceof RemoteRouteWithdrawCommand) {
            RemoteRouteWithdrawCommand withdrawCommand = (RemoteRouteWithdrawCommand) message;
            handleWithdrawRemoteElasticTasks(withdrawCommand);
        } else if (message instanceof String) {
            System.out.println("I received message " + message);
            sendMessageToMaster("I received message " + message);
        } else if (message instanceof ThroughputQueryCommand) {
            ThroughputQueryCommand throughputQueryCommand = (ThroughputQueryCommand) message;
            double throughput = ElasticTaskHolder.instance().getThroughput(throughputQueryCommand.taskid);
            getSender().tell(throughput, getSelf());
        } else if (message instanceof DistributionQueryCommand) {
            DistributionQueryCommand distributionQueryCommand = (DistributionQueryCommand)message;
            Histograms distribution = ElasticTaskHolder.instance().getDistribution(distributionQueryCommand.taskid);
            getSender().tell(distribution, getSelf());
        } else if (message instanceof RoutingTableQueryCommand) {
            RoutingTableQueryCommand queryCommand = (RoutingTableQueryCommand)message;
            RoutingTable queryResult = ElasticTaskHolder.instance().getRoutingTable(queryCommand.taskid);
            getSender().tell(queryResult, getSelf());
        } else if (message instanceof ReassignBucketToRouteCommand) {
            System.out.println("I received ReassignBucketToRouteCommand message " + message);
            ReassignBucketToRouteCommand reassignBucketToRouteCommand = (ReassignBucketToRouteCommand) message;
            ElasticTaskHolder.instance().reassignHashBucketToRoute(reassignBucketToRouteCommand.taskId, reassignBucketToRouteCommand.bucketId,
                    reassignBucketToRouteCommand.originalRoute, reassignBucketToRouteCommand.newRoute);
        } else if (message instanceof BucketDistributionQueryCommand) {
            System.out.println("I received BucketDistributionQueryCommand!");

            BucketDistributionQueryCommand command = (BucketDistributionQueryCommand) message;
            getSender().tell(ElasticTaskHolder.instance().getBucketDistributionForBalancedRoutingTable(command.taskid), getSelf());
        } else {
            System.out.println("[Elastic]: Unknown message.");
            unhandled(message);
        }
    }

    ElasticTaskMigrationMessage addIpInfo(ElasticTaskMigrationMessage message, String address) {
        message._ip = extractIpFromActorAddress(address);
        return message;
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

    void register(Member member) {
        if(member.hasRole("master")) {
            _master = getContext().actorSelection(member.address()+"/user/master");
            _master.tell(new HelloMessage(_name, _port),getSelf());
            System.out.println("I have sent registration message to master.");
        } else if (member.hasRole("slave")) {
            getContext().actorSelection(member.address()+"/user/slave")
                    .tell(new HelloMessage(_name, _port),getSelf());
            System.out.format("I have sent registration message to %s\n", member.address());
//            sendMessageToMaster("I have sent registration message to "+ member.address());
        }
    }

    public void sendMessageToMaster(String message) {
        _master.tell(new LogMessage(message, _name ), getSelf());
    }

    public void registerOriginalElasticTaskToMaster(int taskId) {
        _master.tell(new ElasticTaskRegistrationMessage(taskId, _name),getSelf());
    }

    private void handleTaskMigrationCommandMessage(TaskMigrationCommand taskMigrationCommand) {
        if(!_nameToPath.containsKey(taskMigrationCommand._targetHostName)) {
            System.out.println("[Elastic]:target host "+ taskMigrationCommand._targetHostName+"does not exist!");
            sendMessageToMaster(taskMigrationCommand._targetHostName+" does not exist! Valid names are "+_nameToPath.keySet());
            return;
        }
        try{
            ElasticTaskMigrationMessage migrationMessage = ElasticTaskHolder.instance().generateRemoteElasticTasks(taskMigrationCommand._taskID, taskMigrationCommand._route);
//            _nameToPath.get(taskMigrationCommand._targetHostName).tell(migrationMessage, getSelf());
            if(migrationMessage!=null) {
                getContext().actorFor(_nameToPath.get(taskMigrationCommand._targetHostName)).tell(migrationMessage, getSelf());
                System.out.println("[Elastic]: elastic message has been sent to "+_nameToPath.get(taskMigrationCommand._targetHostName)+"["+_nameToPath.get(taskMigrationCommand._targetHostName)+"]");
//                sendMessageToMaster("I have passed the elastic message to "+ taskMigrationCommand._targetHostName+"["+_nameToPath.get(taskMigrationCommand._targetHostName)+"]");
            } else {
                throw new RuntimeException("generateRemoteElasticTasks returns null!");
            }
        } catch (Exception e) {
            sendMessageToMaster(e.getMessage());
//            ("I do not contains the task for task id"+ taskMigrationCommand._taskID,null);
        }
//        if(migrationMessage!=null) {
//            System.out.print("The number of routes in the generated elastic tasks:"+migrationMessage._elasticTask.get_routingTable().getRoutes().size());
//
//        } else {
//        }
    }

    private void handleElasticTaskMigrationMessage(ElasticTaskMigrationMessage elasticTaskMigrationMessage) {
        System.out.println("[Elastic]: received elastic mask migration message from"+getSender());
//        sendMessageToMaster("[Elastic]: received elastic mask migration message from"+getSender());
//        _master.tell("I received elastic mask migration message from "+getSender().path(),getSelf());
        ElasticTaskMigrationConfirmMessage confirmMessage = ElasticTaskHolder.instance().handleGuestElasticTasks(addIpInfo(elasticTaskMigrationMessage,getSender().path().toString()));
//        _master.tell("I generate confirm message ",getSelf());


        registerRemoteRoutesOnMaster(elasticTaskMigrationMessage._elasticTask.get_taskID(), elasticTaskMigrationMessage._elasticTask.get_routingTable().getRoutes());

        if(confirmMessage!=null) {
            getSender().tell(confirmMessage, getSelf());
            sendMessageToMaster("I have handled the mask migration message");
//            _master.tell("I have handled the mask migration message",getSelf());
        } else {
            System.err.println("Failed to deploy remote elastic tasks!");
            _master.tell("Failed to deploy elastic tasks", null);
        }
    }

    private void handleElasticTaskMigrationConfirmMessage(ElasticTaskMigrationConfirmMessage confirmMessage) {
        confirmMessage._ip=extractIpFromActorAddress(getSender().path().toString());

        String ip = confirmMessage._ip;
        int port = confirmMessage._port;
        int taskId = confirmMessage._taskId;

        ElasticTaskHolder holder = ElasticTaskHolder.instance();
        System.out.print("Received ElasticTaskMigrationConfirmMessage #. routes: "+confirmMessage._routes.size());
        for(int i: confirmMessage._routes) {
            holder.establishConnectionToRemoteTaskHolder(taskId, i, ip, port);
        }
    }

    private void handleRoutingCreatingCommand(RoutingCreatingCommand creatingCommand) {
        try {
            sendMessageToMaster("begin to handle crate routing command!");
            ElasticTaskHolder.instance().createRouting(creatingCommand._task, creatingCommand._numberOfRoutes, creatingCommand._routingType);
        } catch (Exception e) {
            sendMessageToMaster(e.getMessage());
        }
    }

    private void handleWithdrawRemoteElasticTasks(RemoteRouteWithdrawCommand withdrawCommand) {
        try {
            ElasticTaskHolder.instance().withdrawRemoteElasticTasks( withdrawCommand.taskId, withdrawCommand.route);
        } catch (Exception e) {
            sendMessageToMaster(e.getMessage());
        }
    }

    static public Slave createActor(String name, String port) {
        try{
            final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                    .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostAddress()))
                    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                    .withFallback(ConfigFactory.load());
            ActorSystem system = ActorSystem.create("ClusterSystem", config); 
            system.actorOf(Props.create(Slave.class, name, port), "slave");

            System.out.println("Slave actor is created!");
//            Slave slave = Slave.getInstance();



            return Slave.waitAndGetInstance();
        } catch (UnknownHostException e ) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Slave.class, args[0]), "slave");

    }

    public void registerRemoteRoutesOnMaster(int taskid, int route) {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(route);
        registerRemoteRoutesOnMaster(taskid, routes);
    }

    public void registerRemoteRoutesOnMaster(int taskid, ArrayList<Integer> routes) {
        RemoteRouteRegistrationMessage registrationMessage = new RemoteRouteRegistrationMessage(taskid, routes, _name);
        _master.tell(registrationMessage, getSelf());
    }

    public void unregisterRemoteRoutesOnMaster(int taskid, ArrayList<Integer> routes) {
        RemoteRouteRegistrationMessage registrationMessage = new RemoteRouteRegistrationMessage(taskid, routes, _name);
        registrationMessage.setUnregister();
        _master.tell(registrationMessage, getSelf());
    }

    public void unregisterRemoteRoutesOnMaster(int taskid, int route) {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(route);
        unregisterRemoteRoutesOnMaster(taskid, routes);
    }

}
