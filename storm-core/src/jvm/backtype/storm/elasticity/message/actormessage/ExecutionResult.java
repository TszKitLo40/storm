package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 12/28/15.
 */
public class ExecutionResult implements IMessage {

    public enum Status {Succeed, Fail};

    public Status result;

    public String msg;

    private ExecutionResult(Status status, String message) {
        this.msg = message;
        this.result = status;
    }

    private ExecutionResult(Status status) {
        this.result = status;
    }

    public static ExecutionResult Succeed() {
        return new ExecutionResult(Status.Succeed);
    }

    public static ExecutionResult Fail(String reason) {
        return new ExecutionResult(Status.Fail, reason);
    }

}
