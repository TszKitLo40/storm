package backtype.storm.elasticity;

import backtype.storm.elasticity.ActorFramework.Message.ElasticTaskMigrationConfirmMessage;
import backtype.storm.elasticity.ActorFramework.Message.ElasticTaskMigrationMessage;
import backtype.storm.elasticity.ActorFramework.Slave;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.exceptions.TaskNotExistingException;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.state.*;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.netty.Context;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    public static Logger LOG = LoggerFactory.getLogger(ElasticTaskHolder.class);

    private static ElasticTaskHolder _instance;

    private IContext _context;

    private IConnection _inputConnection;

    private String _stormId;

    private int _port;

    public Slave _slaveActor;

    Map<Integer, BaseElasticBoltExecutor> _bolts = new HashMap<>();

    Map<Integer, ElasticTasks> _remoteTasks = new HashMap<>();

    Map<Integer, ElasticRemoteTaskExecutor> _originalTaskIdToRemoteTaskExecutor = new HashMap<>();

    Map<Integer, IConnection> _originalTaskIdToConnection = new HashMap<>();

    LinkedBlockingQueue<ITaskMessage> _sendingQueue = new LinkedBlockingQueue<>();

    private LinkedBlockingQueue<ITaskMessage> _remoteTupleOutputQueue = new LinkedBlockingQueue<>();

    private Map<String, IConnection> _taskidRouteToConnection = new HashMap<>();

    static public ElasticTaskHolder instance() {
        return _instance;
    }

    static public ElasticTaskHolder createAndGetInstance(Map stormConf, String stormId, int port) {
        if(_instance==null) {
            _instance = new ElasticTaskHolder(stormConf, stormId, port);
        }
        return _instance;
    }



    private ElasticTaskHolder(Map stormConf, String stormId, int port) {
        System.out.println("creating ElasticTaskHolder");
        _context = new Context();
        _port = port + 10000;
        _context.prepare(stormConf);
        _inputConnection = _context.bind(stormId,_port);
        _stormId = stormId;
        _slaveActor = Slave.createActor(_stormId,Integer.toString(port));
        createExecuteResultReceivingThread();
        createExecuteResultSendingThread();
        LOG.info("ElasticTaskHolder is launched.");
        LOG.info("storm id:"+stormId+" port:" + port);
    }

    public void registerElasticBolt(BaseElasticBoltExecutor bolt, int taskId) {
        _bolts.put(taskId, bolt);
        LOG.info("A new ElasticTask is registered." + taskId);
    }

    public void sendMessageToMaster(String message) {
        _slaveActor.sendMessageToMaster(message);
    }


    public ElasticTaskMigrationMessage generateRemoteElasticTasks(int taskid, int route) {
        if(!_bolts.containsKey(taskid)){
            System.err.println("task )"+taskid+"does not exist!");
            return null;
        }

        /* set exceptions for existing routing table and get the complement routing table */
        PartialHashingRouting complementHashingRouting = _bolts.get(taskid).get_elasticTasks().addExceptionForHashRouting(route, _sendingQueue);

        if(complementHashingRouting==null) {
            return null;
        }

        /* construct the instance of ElasticTasks to be executed remotely */
        ElasticTasks existingElasticTasks = _bolts.get(taskid).get_elasticTasks();
        ElasticTasks remoteElasticTasks = new ElasticTasks(existingElasticTasks.get_bolt(),existingElasticTasks.get_taskID());
        remoteElasticTasks.set_routingTable(complementHashingRouting);

        KeyValueState existingState = existingElasticTasks.get_bolt().getState();

        KeyValueState state = new KeyValueState();

        for(Object key: existingState.getState().keySet()) {
            if(complementHashingRouting.route(key)>=0) {
                state.setValueBySey(key, existingState.getValueByKey(key));
                System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be migrated!");
            } else {
                System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be ignored!");
            }
        }

        return new ElasticTaskMigrationMessage(remoteElasticTasks, _port, state);
    }

    public ElasticTaskMigrationConfirmMessage handleGuestElasticTasks(ElasticTaskMigrationMessage message) {
        System.out.println("ElasticTaskMigrationMessage: "+ message.getString());
        System.out.println("#. of routes"+message._elasticTask.get_routingTable().getRoutes().size());
//        _remoteTasks.put(message._elasticTask.get_taskID(), message._elasticTask);
        IConnection iConnection = _context.connect(message._ip + ":" + message._port + "-" + message._elasticTask.get_taskID(),message._ip,message._port);
        _originalTaskIdToConnection.put(message._elasticTask.get_taskID(),iConnection);
        System.out.println("Connected with orignal Task Holders");

        if(!_originalTaskIdToRemoteTaskExecutor.containsKey(message._elasticTask.get_taskID())) {
            //This is the first RemoteTasks assigned to this host.

            ElasticRemoteTaskExecutor remoteTaskExecutor = new ElasticRemoteTaskExecutor(message._elasticTask, _sendingQueue, message._elasticTask.get_bolt());

            System.out.println("ElasticRemoteTaskExecutor is created!");

            _originalTaskIdToRemoteTaskExecutor.put(message._elasticTask.get_taskID(), remoteTaskExecutor);

            System.out.println("ElasticRemoteTaskExecutor is added to the map!");
            remoteTaskExecutor.prepare(message.state);
            System.out.println("ElasticRemoteTaskExecutor is prepared!");
//            remoteTaskExecutor.createProcessingThread();
            System.out.println("Remote Task Executor is launched");
        } else {
            //There is already a RemoteTasks for that tasks on this host, so we just need to update the routing table
            //and create processing thread accordingly.


            ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(message._elasticTask.get_taskID());
            remoteTaskExecutor._elasticTasks.get_bolt().getState().update(message.state);
            for(Object key: message.state.getState().keySet()) {
                System.out.println("State <"+key+", "+ message.state.getValueByKey(key)+"> has been restored!");
            }
            remoteTaskExecutor.mergeRoutingTableAndCreateCreateWorkerThreads(message._elasticTask.get_routingTable());


        }
//
//        System.out.println("I have established IConnection with " + message._ip + ":" + message._port );
//        byte[] bytes = new byte[5];
//
//        iConnection.send(message._elasticTask.get_taskID(), SerializationUtils.serialize("string"));
//        System.out.println("I have sent something to " + message._ip + ":" + message._port );

        ElasticTaskMigrationConfirmMessage confirmMessage = new ElasticTaskMigrationConfirmMessage(message._elasticTask.get_taskID(),"",_port, message._elasticTask.get_routingTable().getRoutes() );


        return confirmMessage;

    }

    private void createExecuteResultSendingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        System.out.println("Try to fetch an element from sendingQueue");
                        ITaskMessage message = _sendingQueue.take();
                        System.out.println("An element is taken from the sendingQueue");
                        if(message instanceof RemoteTupleExecuteResult) {
                            System.out.println("The element is RemoteTupleExecuteResult");

                            RemoteTupleExecuteResult remoteTupleExecuteResult = (RemoteTupleExecuteResult)message;
                            if (_originalTaskIdToConnection.containsKey(remoteTupleExecuteResult._originalTaskID)) {
                                byte[] bytes = SerializationUtils.serialize(remoteTupleExecuteResult);
                                _originalTaskIdToConnection.get(remoteTupleExecuteResult._originalTaskID).send(remoteTupleExecuteResult._originalTaskID, bytes);
                                System.out.println("RemoteTupleExecutorResult is send back!");
                            } else {
                                System.err.println("RemoteTupleExecuteResult will be ignored, because we cannot find the connection for tasks " + remoteTupleExecuteResult._originalTaskID);
                            }
                        } else if (message instanceof RemoteTuple) {
                            System.out.println("The element is RemoteTuple");
                            RemoteTuple remoteTuple = (RemoteTuple) message;
                            final String key = remoteTuple.taskIdAndRoutePair();
                            System.out.println("Key :"+key);
                            if(_taskidRouteToConnection.containsKey(key)) {
                                System.out.println("The element will be serialized!");
                                final byte[] bytes = SerializationUtils.serialize(remoteTuple);
                                System.out.println("RemoteTuple will be sent!");
                                _taskidRouteToConnection.get(key).send(remoteTuple._taskId, bytes);
                                System.out.println("RemoteTuple is send!");
                            } else {
                                System.err.println("RemoteTuple will be ignored, because we cannot find connection for remote tasks " + remoteTuple.taskIdAndRoutePair());
                            }

                        } else if (message instanceof RemoteState) {
                            RemoteState remoteState = (RemoteState) message;
                            if(_originalTaskIdToRemoteTaskExecutor.containsKey(remoteState._taskId)) {
                                byte[] bytes = SerializationUtils.serialize(remoteState);
                                _originalTaskIdToConnection.get(remoteState._taskId).send(remoteState._taskId,bytes);
                                System.out.println("RemoteState is sent back!");
                            } else {
                                System.err.println("Cannot find the connection for task " + remoteState._state);
                            }

                        } else {
                            System.err.print("Unknown element from the sending queue");
                        }

                    } catch (InterruptedException e) {
                        System.out.println("sending thread has been interrupted!");
                    } catch (SerializationException ex) {
                        System.err.println("Serialization Error!");
                        ex.printStackTrace();
                    }
                }
            }
        }).start();
        System.out.println("sending thread is created!");
    }

    private void createExecuteResultReceivingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Iterator<TaskMessage> messageIterator = _inputConnection.recv(0, 0);
                    while(messageIterator.hasNext()) {
                        TaskMessage message = messageIterator.next();
                        int targetTaskId = message.task();

                        Object object = SerializationUtils.deserialize(message.message());
                        if(object instanceof RemoteTupleExecuteResult) {
                            RemoteTupleExecuteResult result = (RemoteTupleExecuteResult)object;
                            System.out.println("A query result is received for "+result._originalTaskID);
                            _bolts.get(targetTaskId).insertToResultQueue(result);
                            System.out.println("a query result tuple is added into the input queue");
                        } else if (object instanceof RemoteTuple) {
                            RemoteTuple remoteTuple = (RemoteTuple) object;
                            try {
                                System.out.format("A remote tuple %d.%d is received!\n",remoteTuple._taskId,remoteTuple._route);
                                _originalTaskIdToRemoteTaskExecutor.get(remoteTuple._taskId).get_inputQueue().put(remoteTuple._tuple);
                                System.out.print("A remote tuple is added to the queue!");

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else if (object instanceof RemoteState) {
                            System.out.println("Received RemoteState!");
                            RemoteState remoteState = (RemoteState) object;
                            if(_bolts.containsKey(remoteState._taskId)) {
                                _bolts.get(remoteState._taskId).get_elasticTasks().get_bolt().getState().update(remoteState._state);
                                System.out.println("State ("+remoteState._state.getState().size()+ " elements) has been updated!");
                            } else {
                                System.err.println("Cannot update State, because task ["+remoteState._taskId+"] does not exist");
                            }
                        }

                    }
                }
            }
        }).start();
    }

    public void establishConnectionToRemoteTaskHolder(int taksId, int route, String remoteIp, int remotePort) {
        IConnection connection = _context.connect("",remoteIp,remotePort);
        _taskidRouteToConnection.put(taksId+"."+route, connection);
        System.out.println("Established connection with remote task holder!");
    }

    public void handleRoutingCreation(int taskid, int numberOfRouting, String type) throws TaskNotExistingException,RoutingTypeNotSupportedException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }
        if(!type.equals("hash"))
            throw new RoutingTypeNotSupportedException("Only support hash routing now!");
        _bolts.get(taskid).get_elasticTasks().setHashRouting(numberOfRouting);
        System.out.println("RoutingTable has been created");

    }

}
