package backtype.storm.elasticity.message.actormessage;

import backtype.storm.elasticity.actors.utils.SubtaskLevelLoadBalancing;

/**
 * Created by robert on 1/29/16.
 */
public class SubtaskLevelLoadBalancingCommand implements ICommand {
    public int taskid;
    public SubtaskLevelLoadBalancingCommand(int taskid) {
        this.taskid = taskid;
    }
}
