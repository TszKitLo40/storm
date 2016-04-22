package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 4/12/16.
 */
public class Status implements IMessage {

    public static int OK = 1;
    public static int ERROR = 2;

    public String msg;
    public int code;

    public static Status OK(String msg) {
        Status status = new Status();
        status.code = OK;
        status.msg = msg;
        return status;
    }

    public static Status OK() {
        return OK(null);
    }

    public static Status Error(String msg) {
        Status status = new Status();
        status.code = ERROR;
        status.msg = msg;
        return status;
    }

    public static Status Error() {
        return Error(null);
    }

    public String toString() {
        String ret = "";
        if(code == ERROR) {
            ret += "code: ERROR\n";
        } else if (code == OK) {
            ret += "code: OK\n";
        }
        ret += "msg: " + msg;
        return ret;
    }
}