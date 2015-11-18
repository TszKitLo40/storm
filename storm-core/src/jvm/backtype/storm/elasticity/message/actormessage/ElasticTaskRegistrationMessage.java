package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/17/15.
 */
public class ElasticTaskRegistrationMessage implements IMessage {

    public int taskId;
    public String hostName;

    public ElasticTaskRegistrationMessage(int taskId, String hostName) {
        this.taskId = taskId;
        this.hostName = hostName;
    }

}
