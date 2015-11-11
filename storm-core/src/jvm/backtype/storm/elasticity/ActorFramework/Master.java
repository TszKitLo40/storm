package backtype.storm.elasticity.ActorFramework;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent;
import backtype.storm.elasticity.ActorFramework.Message.HelloMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor {

    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    static Master _instance;

    public static Master getInstance() {
        return _instance;
    }

    public Master() {
        _instance = this;
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
}
