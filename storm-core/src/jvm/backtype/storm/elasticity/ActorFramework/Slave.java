package backtype.storm.elasticity.ActorFramework;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import backtype.storm.elasticity.ActorFramework.Message.HelloMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 11/11/15.
 */
public class Slave extends UntypedActor {

    Cluster cluster = Cluster.get(getContext().system());

    Map<String, ActorRef> _nameToActors = new HashMap<>();

    String _name;

    static Slave _instance;

    public Slave(String name, String port) {
        _name = name+":"+port+"-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
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
        }
        else if (message instanceof HelloMessage) {
            HelloMessage helloMessage = (HelloMessage)message;
            _nameToActors.put(helloMessage.getName(),getSender());
            System.out.format("I am connected with %s\n", ((HelloMessage) message).getName());
        } else {
            System.out.println("Unknown message.");
            unhandled(message);
        }
    }

    void register(Member member) {
        if(member.hasRole("master")) {
            getContext().actorSelection(member.address()+"/user/master")
                    .tell(new HelloMessage(_name),getSelf());
            System.out.println("I have sent registration message to master.");
        } else if (member.hasRole("slave")) {
            getContext().actorSelection(member.address()+"/user/slave")
                    .tell(new HelloMessage(_name),getSender());
            System.out.format("I have sent registration message to %s\n", member.address());
        }
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
