package backtype.storm.elasticity.networking;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.metric.SystemBolt;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by robert on 12/8/15.
 */
public class Receiver implements IConnection {

    ServerSocket server;

    Map<ObjectInputStream, Socket> inputStreamSocketMap = new HashMap<ObjectInputStream, Socket>();

    Map<ObjectInputStream, Thread> inputStreamThreadMap = new HashMap<>();

    LinkedBlockingQueue<TaskMessage> inputData = new LinkedBlockingQueue<>(10);
    final int maxBatchSize = 1000;

    public Receiver(int port) {
        try {
            server = new ServerSocket(port);

        } catch (Exception e) {
            e.printStackTrace();
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket newsocket = server.accept();
                        System.out.println("Receiver: New connection established!");
                        final ObjectInputStream newInputStream = new ObjectInputStream(newsocket.getInputStream());
                        inputStreamSocketMap.put(newInputStream, newsocket);


                        Thread receivingThread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                int receiveCount = 0;
                                while (true) {
                                    try {
                                        TaskMessage message = (TaskMessage)(newInputStream.readObject());
                                        message.set_name("Received Message");
                                        inputData.put(message);
                                        receiveCount++;
//                                        System.out.println("received!!");
//                                        if(receiveCount%10000==0)
//                                            newInputStream.reset();
//                                        if(new Random().nextFloat()<0.001)
//                                            newInputStream.reset();

                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    } catch (IOException ee) {
                                        break;
                                    } catch (InterruptedException inter) {
                                        inter.printStackTrace();
                                        break;
                                    }
                                }
                            }
                        });
                        receivingThread.start();
                        inputStreamThreadMap.put(newInputStream, receivingThread);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }).start();
    }

    public TaskMessage recv() throws InterruptedException{
        return inputData.take();
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {

//        ArrayList<TaskMessage> taskMessages = new ArrayList<>();
//        inputData.drainTo(taskMessages, maxBatchSize);
//        return taskMessages.iterator();

        ArrayList<TaskMessage> taskMessages = new ArrayList<TaskMessage>();
//        int received = 0;
//        while(received < maxBatchSize) {
//            taskMessages.add(inputData.drainTo());
//        }
//        System.out.println("recv() is called!");
//        inputData.drainTo(taskMessages, maxBatchSize);
        try {
            taskMessages.add(inputData.take());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        System.out.println("input data size: " + inputData.size());
//        System.out.println(taskMessages.size() + " message is created!");
        return taskMessages.iterator();
    }

    @Override
    public void send(int taskId, byte[] payload) {
        throw new UnsupportedOperationException("send is not supported by Receiver");
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        throw new UnsupportedOperationException("send is not supported by Receiver");
    }

    @Override
    public void close() {
        try {
            server.close();
            for (Socket socket : inputStreamSocketMap.values()) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}