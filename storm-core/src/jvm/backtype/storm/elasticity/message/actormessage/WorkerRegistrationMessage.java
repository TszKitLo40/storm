package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/11/15.
 */
public class WorkerRegistrationMessage implements IMessage {

    String _name;

    int _port;

    int _numberOfProcossors;

    public WorkerRegistrationMessage(String name, int port) {
        this(name, port, -1);
    }

    public WorkerRegistrationMessage(String name, int port, int numberOfProcessors) {
        _name = name;
        _port = port;
        _numberOfProcossors = numberOfProcessors;
    }

    public String getName() {
        return _name;
    }

    public int getPort() {
        return _port;
    }

    public int getNumberOfProcessors() {
        return _numberOfProcossors;
    }
}
