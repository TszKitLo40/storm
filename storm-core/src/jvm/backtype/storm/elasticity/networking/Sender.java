package backtype.storm.elasticity.networking;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Iterator;

/**
 * Created by robert on 12/8/15.
 */
public class Sender implements IConnection {

    Socket socket;
    ObjectOutputStream out;
    int sendCount = 0;

    public Sender(String ip, int port) {

        try {
            socket = new Socket(ip, port);
            out = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new UnsupportedOperationException("recv() is unsupported in Sender!");
//        return null;
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage taskMessage = new TaskMessage(taskId, payload);
        try {
            out.writeObject(taskMessage);

            out.flush();
            sendCount++;
            if(sendCount % 1000 == 0) {
                sendCount = 0;
                out.reset();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        while(msgs.hasNext()) {
            try {
                out.writeObject(msgs.next());
                out.flush();
                sendCount++;
//                System.out.println("sent!");
                if(sendCount % 1000 == 0) {
                    sendCount = 0;
                    out.reset();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        try {
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}