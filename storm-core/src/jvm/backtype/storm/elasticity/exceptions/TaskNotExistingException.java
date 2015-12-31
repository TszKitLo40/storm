package backtype.storm.elasticity.exceptions;

/**
 * Created by Robert on 11/17/15.
 */
public class TaskNotExistingException extends Exception {

    public TaskNotExistingException(String reason) {
        super(reason);
    }
    public TaskNotExistingException(int task) {
        super("Tasks " + task + " does not exist!");
    }

}
