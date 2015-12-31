package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/31/15.
 */
public class WorkerLogicalNameMessage implements IMessage {

    public String name;

    public WorkerLogicalNameMessage(String name) {
        this.name = name;
    }

}
