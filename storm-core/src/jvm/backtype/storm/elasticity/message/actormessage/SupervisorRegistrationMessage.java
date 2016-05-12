package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/11/15.
 */
public class SupervisorRegistrationMessage implements IMessage {

    String _name;

    int _port;

    int _numberOfProcossors;

    public SupervisorRegistrationMessage(String name, int port) {
        this(name, port, -1);
    }

    public SupervisorRegistrationMessage(String name, int port, int numberOfProcessors) {
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
