package backtype.storm.elasticity.ActorFramework;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import backtype.storm.elasticity.ActorFramework.Message.ElasticTaskMigrationConfirmMessage;
import backtype.storm.elasticity.ActorFramework.Message.ElasticTaskMigrationMessage;
import backtype.storm.elasticity.ActorFramework.Message.HelloMessage;
import backtype.storm.elasticity.ActorFramework.Message.TaskMigrationCommandMessage;
import backtype.storm.elasticity.ElasticTaskHolder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Slave extends UntypedActor {

    Cluster cluster = Cluster.get(getContext().system());

    Map<String, ActorRef> _nameToActors = new HashMap<>();

    String _name;

    ActorSelection _master;

    static Slave _instance;

    public Slave(String name, String port) {
//        _name = name+":"+port+"-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        _name = name + ":" + port;
        _instance = this;
    }

    public static Slave getInstance() {
        return _instance;
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof ClusterEvent.CurrentClusterState) {
            ClusterEvent.CurrentClusterState state = (ClusterEvent.CurrentClusterState)message;
            for(Member member: state.getMembers()) {
                if(member.status().equals(MemberStatus.up())) {
                    register(member);
                }
            }
        } else if (message instanceof ClusterEvent.MemberUp) {
            ClusterEvent.MemberUp memberUp = (ClusterEvent.MemberUp)message;
            register(memberUp.member());
        } else if (message instanceof HelloMessage) {
            HelloMessage helloMessage = (HelloMessage)message;
            _nameToActors.put(helloMessage.getName(),getSender());
            System.out.println("[Elastic]: I am connected with " + ((HelloMessage) message).getName() +"["+getSender()+"]");
        } else if (message instanceof TaskMigrationCommandMessage) {
            System.out.println("[Elastic]: recieved  TaskMigrationCommandMessage!");
            TaskMigrationCommandMessage taskMigrationCommandMessage = (TaskMigrationCommandMessage) message;

            if(!_nameToActors.containsKey(taskMigrationCommandMessage._targetHostName)) {
                System.out.println("[Elastic]:target host "+taskMigrationCommandMessage._targetHostName+"does not exist!");
                return;
            }

            ElasticTaskMigrationMessage migrationMessage = ElasticTaskHolder.instance().generateRemoteElasticTasks(taskMigrationCommandMessage._taskID, taskMigrationCommandMessage._route);
            System.out.print("The number of routes in the generated elastic tasks:"+migrationMessage._elasticTask.get_routingTable().getRoutes().size());
            if(migrationMessage!=null) {

                _nameToActors.get(taskMigrationCommandMessage._targetHostName).tell(migrationMessage, getSelf());
                System.out.println("[Elastic]: elastic message has been sent to "+_nameToActors.get(taskMigrationCommandMessage._targetHostName)+"["+_nameToActors.get(taskMigrationCommandMessage._targetHostName).path()+"]");
                _master.tell("I have passed the elastic message to "+taskMigrationCommandMessage._targetHostName,null);
            } else {
                _master.tell("I do not contains the task for task id"+taskMigrationCommandMessage._taskID,null);
            }

        } else if (message instanceof ElasticTaskMigrationMessage) {
            System.out.println("[Elastic]: received elastic mask migration message from"+getSender());
            _master.tell("I received elastic mask migration message from "+getSender().path(),getSelf());
            ElasticTaskMigrationConfirmMessage confirmMessage = ElasticTaskHolder.instance().handleGuestElasticTasks(addIpInfo((ElasticTaskMigrationMessage)message,getSender().path().toString()));
            _master.tell("I generate confirm message ",getSelf());

            if(confirmMessage!=null) {
                getSender().tell(confirmMessage, getSelf());
                _master.tell("I have handled the mask migration message",getSelf());
            } else {
                System.err.println("Failed to deploy remote elastic tasks!");
                _master.tell("Failed to deploy elastic tasks", null);
            }
        } else if (message instanceof ElasticTaskMigrationConfirmMessage) {
            ElasticTaskMigrationConfirmMessage confirmMessage = (ElasticTaskMigrationConfirmMessage)message;
            confirmMessage._ip=extractIpFromActorAddress(getSender().path().toString());

            String ip = confirmMessage._ip;
            int port = confirmMessage._port;
            int taskId = confirmMessage._taskId;

            ElasticTaskHolder holder = ElasticTaskHolder.instance();
            System.out.print("Received ElasticTaskMigrationConfirmMessage #. routes: "+confirmMessage._routes.size());
            for(int i: confirmMessage._routes) {
                holder.establishConnectionToRemoteTaskHolder(taskId, i, ip, port);
            }

        } else if (message instanceof String) {
            System.out.println("I received message "+ message);
        }

        else {
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
            _master.tell(new HelloMessage(_name),getSelf());
            System.out.println("I have sent registration message to master.");
        } else if (member.hasRole("slave")) {
            getContext().actorSelection(member.address()+"/user/slave")
                    .tell(new HelloMessage(_name),getSelf());
            System.out.format("I have sent registration message to %s\n", member.address());
        }
    }

    public void sendMessageToMaster(String message) {
        _master.tell(message, getSelf());
    }

    static public Slave createActor(String name, String port) {

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Slave.class, name, port), "slave");

        System.out.println("Slave actor is created!");

        return Slave.getInstance();
    }

    public static void main(String[] args) {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Slave.class, args[0]), "slave");

    }


}
