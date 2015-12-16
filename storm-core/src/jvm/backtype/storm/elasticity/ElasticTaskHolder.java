package backtype.storm.elasticity;

import backtype.storm.elasticity.message.actormessage.ElasticTaskMigrationConfirmMessage;
import backtype.storm.elasticity.message.actormessage.ElasticTaskMigrationMessage;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.exceptions.InvalidRouteException;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.exceptions.TaskNotExistingException;
import backtype.storm.elasticity.message.taksmessage.*;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.state.*;
import backtype.storm.elasticity.utils.FirstFitDoubleDecreasing;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.netty.Context;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    public static Logger LOG = LoggerFactory.getLogger(ElasticTaskHolder.class);

    private static ElasticTaskHolder _instance;

    private IContext _context;

    private IConnection _inputConnection;

    private String _workerId;

    private int _port;

    public Slave _slaveActor;

    private WorkerTopologyContext _workerTopologyContext;

    Map<Integer, BaseElasticBoltExecutor> _bolts = new HashMap<>();

    Map<Integer, ElasticRemoteTaskExecutor> _originalTaskIdToRemoteTaskExecutor = new HashMap<>();

    Map<Integer, IConnection> _originalTaskIdToConnection = new HashMap<>();

    LinkedBlockingQueue<ITaskMessage> _sendingQueue = new LinkedBlockingQueue<>(256);

    Map<String, Semaphore> _taskidRouteToStateWaitingSemaphore = new HashMap<>();

    private LinkedBlockingQueue<ITaskMessage> _remoteTupleOutputQueue = new LinkedBlockingQueue<>(256);

    private Map<String, IConnection> _taskidRouteToConnection = new HashMap<>();

    KryoValuesSerializer serializer;

    static public ElasticTaskHolder instance() {
        return _instance;
    }

    static public ElasticTaskHolder createAndGetInstance(Map stormConf, String workerId, int port) {
        if(_instance==null) {
            _instance = new ElasticTaskHolder(stormConf, workerId, port);
        }
        return _instance;
//        return null;
    }




    private ElasticTaskHolder(Map stormConf, String workerId, int port) {
        serializer = new KryoValuesSerializer(stormConf);
        System.out.println("creating ElasticTaskHolder");
        _context = new Context();
        _port = port + 10000;
        _context.prepare(stormConf);
        _inputConnection = _context.bind(workerId,_port);
        _workerId = workerId;
        _slaveActor = Slave.createActor(_workerId,Integer.toString(port));
        if(_slaveActor == null)
            System.out.println("NOTE: _slaveActor is null!!***************\n");
        createExecuteResultReceivingThread();
        createExecuteResultSendingThread();
        LOG.info("ElasticTaskHolder is launched.");
        LOG.info("storm id:"+workerId+" port:" + port);
        Utils.sleep(2000);
        _slaveActor.sendMessageToMaster("My pid is: " + ManagementFactory.getRuntimeMXBean().getName());
    }

    public void registerElasticBolt(BaseElasticBoltExecutor bolt, int taskId) {
        _bolts.put(taskId, bolt);
        _slaveActor.registerOriginalElasticTaskToMaster(taskId);
        LOG.info("A new ElasticTask is registered." + taskId);
    }

    public void sendMessageToMaster(String message) {
        _slaveActor.sendMessageToMaster(message);
    }


    public ElasticTaskMigrationMessage generateRemoteElasticTasks(int taskid, int route) throws RoutingTypeNotSupportedException, InvalidRouteException {
        if(!_bolts.containsKey(taskid)){
            System.err.println("task )"+taskid+"does not exist!");
            return null;
        }

        System.out.println("Add exceptions to the routing table...");
        /* set exceptions for existing routing table and get the complement routing table */
        PartialHashingRouting complementHashingRouting = _bolts.get(taskid).get_elasticTasks().addExceptionForHashRouting(route, _sendingQueue);

        if(complementHashingRouting==null) {
            return null;
        }

        System.out.println("Constructing the instance of ElasticTask for remote execution...");
        /* construct the instance of ElasticTasks to be executed remotely */
        ElasticTasks existingElasticTasks = _bolts.get(taskid).get_elasticTasks();
        ElasticTasks remoteElasticTasks = new ElasticTasks(existingElasticTasks.get_bolt(),existingElasticTasks.get_taskID());
        remoteElasticTasks.set_routingTable(complementHashingRouting);

        System.out.println("Packing the involved state...");
        KeyValueState existingState = existingElasticTasks.get_bolt().getState();

        KeyValueState state = new KeyValueState();

        for(Object key: existingState.getState().keySet()) {
            if(complementHashingRouting.route(key)>=0) {
                state.setValueBySey(key, existingState.getValueByKey(key));
//                System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be migrated!");
            } else {
//                System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be ignored!");
            }
        }
        System.out.println("State for migration is ready!");

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
//            for(Object key: message.state.getState().keySet()) {
//                System.out.println("State <"+key+", "+ message.state.getValueByKey(key)+"> has been restored!");
//            }
            System.out.println("Received original state!");
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
                Integer count = 0;
                while (true) {
                    try {

//                            byte[] bytess = SerializationUtils.serialize(new FakeTuple());
//                            ArrayList<TaskMessage> taskMessagess = new ArrayList<>();
//                            taskMessagess.add(new TaskMessage(5, bytess));
//                            _originalTaskIdToConnection.get(5).send(taskMessagess.iterator());
//
//                            count++;
//
//                            if(count % 1000000 == 0 ) {
//                                System.out.println("Send " + count + " tuples!");
//                                count = 0 ;
//                            }


//
                        final ITaskMessage message = _sendingQueue.take();
                        LOG.debug("An element is taken from the sendingQueue");

                        if(message instanceof RemoteTupleExecuteResult) {
                            LOG.debug("The element is RemoteTupleExecuteResult");

                            RemoteTupleExecuteResult remoteTupleExecuteResult = (RemoteTupleExecuteResult)message;
                            if (_originalTaskIdToConnection.containsKey(remoteTupleExecuteResult._originalTaskID)) {
                                byte[] bytes = SerializationUtils.serialize(remoteTupleExecuteResult);
                                final ArrayList<TaskMessage> taskMessages = new ArrayList<>();
                                taskMessages.add(new TaskMessage(remoteTupleExecuteResult._originalTaskID, bytes, "RemoteTupleExecuteResult"));
                                _originalTaskIdToConnection.get(remoteTupleExecuteResult._originalTaskID).send(taskMessages.iterator());
                                taskMessages.clear();
//                                _originalTaskIdToConnection.get(remoteTupleExecuteResult._originalTaskID).send(remoteTupleExecuteResult._originalTaskID, SerializationUtils.serialize(remoteTupleExecuteResult));

                                LOG.debug("RemoteTupleExecutorResult is send back!");
                            } else {
                                System.err.println("RemoteTupleExecuteResult will be ignored, because we cannot find the connection for tasks " + remoteTupleExecuteResult._originalTaskID);
                            }
                        } else if (message instanceof RemoteTuple) {

                            LOG.debug("The element is RemoteTuple");
                            RemoteTuple remoteTuple = (RemoteTuple) message;
                            final String key = remoteTuple.taskIdAndRoutePair();
                            LOG.debug("Key :"+key);
                            if(_taskidRouteToConnection.containsKey(key)) {
                                LOG.debug("The element will be serialized!");
                                final byte[] bytes = SerializationUtils.serialize(remoteTuple);
//                                _taskidRouteToConnection.get(key).send(remoteTuple._taskId, SerializationUtils.serialize(remoteTuple));
                                final ArrayList<TaskMessage> taskMessages = new ArrayList<>();
                                taskMessages.add(new TaskMessage(remoteTuple._taskId, bytes, "RemoteTuple"));
                                _taskidRouteToConnection.get(key).send(taskMessages.iterator());
                                taskMessages.clear();
//                                if(new Random().nextDouble()<0.0001) {
//                                    System.out.println(count++ + " tuple has been send!");
//                                    count=0;
//                                    System.out.println("State:" + ((Client)_taskidRouteToConnection.get(key)).getState());
//                                }

                                LOG.debug("RemoteTuple is sent!");
                            } else {
                                System.err.println("RemoteTuple will be ignored, because we cannot find connection for remote tasks " + remoteTuple.taskIdAndRoutePair());
                            }

                        } else if (message instanceof RemoteSubtaskTerminationToken) {
                            RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = (RemoteSubtaskTerminationToken) message;
                            final String key = remoteSubtaskTerminationToken.taskid + "." + remoteSubtaskTerminationToken.route;
                            if(_taskidRouteToConnection.containsKey(key)) {
                                final byte[] bytes = SerializationUtils.serialize(remoteSubtaskTerminationToken);
//                                _taskidRouteToConnection.get(key).send(remoteSubtaskTerminationToken.taskid, SerializationUtils.serialize(remoteSubtaskTerminationToken));
                                final ArrayList<TaskMessage> taskMessages = new ArrayList<>();
                                taskMessages.add(new TaskMessage(remoteSubtaskTerminationToken.taskid, bytes, "RemoteSubtaskTerminationToken"));
                                _taskidRouteToConnection.get(key).send(taskMessages.iterator());
                                taskMessages.clear();

                                System.out.println("RemoteSubtaskTerminationToken is sent");
                            } else {
                                System.err.println("RemoteSubtaskTerminationToken does not have a valid taskid and route!");
                            }

                        } else if (message instanceof RemoteState) {
                            RemoteState remoteState = (RemoteState) message;
                            if(_originalTaskIdToRemoteTaskExecutor.containsKey(remoteState._taskId)) {
                                byte[] bytes = SerializationUtils.serialize(remoteState);
//                                _originalTaskIdToConnection.get(remoteState._taskId).send(remoteState._taskId,SerializationUtils.serialize(remoteState));
                                final ArrayList<TaskMessage> taskMessages = new ArrayList<>();
                                taskMessages.add(new TaskMessage(remoteState._taskId, bytes, "RemoteState" ));
                                _originalTaskIdToConnection.get(remoteState._taskId).send(taskMessages.iterator());
                                taskMessages.clear();

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
                int count = 0;
                while (true) {

//                    Iterator<TaskMessage> taskMessageIterator = _inputConnection.recv(0,0);
//                    while(taskMessageIterator.hasNext()) {
//                        taskMessageIterator.next();
//                        count++;
//                        if(count % 1000000 == 0 ) {
//                            System.out.println("Received " + count + " tuples!");
//                        }
//                    }


                    Iterator<TaskMessage> messageIterator = _inputConnection.recv(0, 0);
//                    if(messageIterator!=null) {
//                        continue;
//                    }
//                    System.out.println("Got something: " + messageIterator);
                    while(messageIterator.hasNext()) {
                        TaskMessage message = messageIterator.next();
                        int targetTaskId = message.task();
//                        Object object = serializer.
                        Object object = SerializationUtils.deserialize(message.message());
                        if(object instanceof RemoteTupleExecuteResult) {
                            message.set_name("Received RemoteTupleExecuteResult");
                            RemoteTupleExecuteResult result = (RemoteTupleExecuteResult)object;
                            ((TupleImpl)result._inputTuple).setContext(_workerTopologyContext);
                            LOG.debug("A query result is received for "+result._originalTaskID);
                            _bolts.get(targetTaskId).insertToResultQueue(result);
                            LOG.debug("a query result tuple is added into the input queue");
                        } else if (object instanceof RemoteTuple) {
                            message.set_name("Received RemoteTuple");
                            RemoteTuple remoteTuple = (RemoteTuple) object;
                            try {
                                LOG.debug("A remote tuple %d.%d is received!\n",remoteTuple._taskId,remoteTuple._route);
                                ((TupleImpl)remoteTuple._tuple).setContext(_workerTopologyContext);
                                _originalTaskIdToRemoteTaskExecutor.get(remoteTuple._taskId).get_inputQueue().put(remoteTuple._tuple);
                                LOG.debug("A remote tuple is added to the queue!");

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else if (object instanceof RemoteState) {
                            System.out.println("Received RemoteState!");
                            message.set_name("Received RemoteState");
                            RemoteState remoteState = (RemoteState) object;
                            if(_bolts.containsKey(remoteState._taskId)) {
                                _bolts.get(remoteState._taskId).get_elasticTasks().get_bolt().getState().update(remoteState._state);
                                System.out.println("State ("+remoteState._state.getState().size()+ " elements) has been updated!");
                                if(remoteState.finalized) {
                                    for(int route: remoteState._routes) {
                                        _taskidRouteToStateWaitingSemaphore.get(remoteState._taskId+"."+route).release();
                                        System.out.println("Semaphore for "+remoteState._taskId+"."+route +"has been released" + message.name());
                                    }
                                }

                            } else {
                                System.err.println("Cannot update State, because task ["+remoteState._taskId+"] does not exist");
                            }
                        } else if (object instanceof RemoteSubtaskTerminationToken) {
                            System.out.print("Received a RemoteSubtaskTerminationToken!");
                            RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = (RemoteSubtaskTerminationToken) object;
                            terminateRemoteRoute(remoteSubtaskTerminationToken.taskid, remoteSubtaskTerminationToken.route);
                            _slaveActor.unregisterRemoteRoutesOnMaster(remoteSubtaskTerminationToken.taskid, remoteSubtaskTerminationToken.route);
                        } else {
                            System.err.println("Unexpected Object: " + object);
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

    public void createRouting(int taskid, int numberOfRouting, String type) throws TaskNotExistingException,RoutingTypeNotSupportedException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }

        if(type.equals("balanced_hash")) {
            createBalancedHashRouting(taskid, numberOfRouting);
        } else {

            if(!type.equals("hash"))
                throw new RoutingTypeNotSupportedException("Only support hash routing now!");
            _bolts.get(taskid).get_elasticTasks().setHashRouting(numberOfRouting);
        }
        _slaveActor.sendMessageToMaster("New RoutingTable has been created!");
        System.out.println("RoutingTable has been created");


    }

    public void createBalancedHashRouting(int taskid, int numberOfRouting) throws TaskNotExistingException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }
        _slaveActor.sendMessageToMaster("phase 1");
        _bolts.get(taskid)._keyBucketSampler.clear();
        _bolts.get(taskid)._keyBucketSampler.enable();
        _slaveActor.sendMessageToMaster("phase 2");
        Utils.sleep(5000);
        _bolts.get(taskid)._keyBucketSampler.disable();
        _slaveActor.sendMessageToMaster("phase 3");
//        _slaveActor.sendMessageToMaster("Distribution: " + _bolts.get(taskid)._keyBucketSampler.toString());

        FirstFitDoubleDecreasing firstFitDoubleDecreasing = new FirstFitDoubleDecreasing(Arrays.asList(_bolts.get(taskid)._keyBucketSampler.buckets),numberOfRouting);

        final int result = firstFitDoubleDecreasing.getResult();
        if(result == numberOfRouting) {
            _slaveActor.sendMessageToMaster(firstFitDoubleDecreasing.toString());
            _bolts.get(taskid).get_elasticTasks().setHashBalancedRouting(numberOfRouting, firstFitDoubleDecreasing.getBucketToPartitionMap());



        } else {
            _slaveActor.sendMessageToMaster("Failed to partition the buckets!");
        }


    }

    public void withdrawRemoteElasticTasks(int taskid, int route) throws TaskNotExistingException, RoutingTypeNotSupportedException, InvalidRouteException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }
        RoutingTable routingTable = _bolts.get(taskid).get_elasticTasks().get_routingTable();
        if(!(routingTable instanceof PartialHashingRouting)) {
            throw new RoutingTypeNotSupportedException("Can only withdraw remote tasks for PartialHashingRouting!");
        }
        PartialHashingRouting partialHashingRouting = (PartialHashingRouting) routingTable;
        if(!partialHashingRouting.getOriginalRoutes().contains(route)) {
            throw new InvalidRouteException("Route " + route + " is not valid");
        }
        if(partialHashingRouting.getRoutes().contains(route)) {
            throw new InvalidRouteException("Route " + route + " is not in exception list");
        }

        _bolts.get(taskid).get_elasticTasks().addValidRoute(route);
        System.out.println("Route "+ route +" has been added into the routing table!");
        sendFinalTuple(taskid, route);
        System.out.println("RemoteSubtaskTerminationToken has been sent!");


        _taskidRouteToStateWaitingSemaphore.put(taskid+ "."+route,new Semaphore(0));
        try {
            System.out.println("Waiting for the remote state");
            _taskidRouteToStateWaitingSemaphore.get(taskid + "." + route).acquire();

            System.out.println("Remote state arrives!");

            System.out.println("launch the thread for "+taskid+"."+route+".");
            _bolts.get(taskid).get_elasticTasks().launchElasticTasksForGivenRoute(route);
            System.out.println("Remote " + taskid + "." + route + "has been withdrawn!");
            _slaveActor.sendMessageToMaster("Remote " + taskid + "." + route + "has been withdrawn!");
        } catch (InterruptedException e ) {

        }
    }

    private void sendFinalTuple(int taskid, int route) {

        RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = new RemoteSubtaskTerminationToken(taskid, route);
        try {
            _sendingQueue.put(remoteSubtaskTerminationToken);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * First terminate the query thread and clear the associated resources until all the local tuples for the route has been processed
     * Then get the state for the route and send the state back to the original ElasticTasks.
     * Finally, remove the route in the routingTable.
     * @param taskid the taskid
     * @param route the route to remove
     */
    private void terminateRemoteRoute(int taskid, int route) {
        //terminate the thread and cleanup the resources.
        ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(taskid);
        remoteTaskExecutor._elasticTasks.terminateGivenQuery(route);
        try {
            //get state and send back
            RemoteState state = remoteTaskExecutor.getStateForRoutes(route);
            state.markAsFinalized();
            _sendingQueue.put(state);
            System.out.println("Final State for " + taskid +"." + route + " has been sent back");


        } catch (InterruptedException e ) {

        }

        ((PartialHashingRouting)remoteTaskExecutor._elasticTasks.get_routingTable()).addExceptionRoute(route);
        System.out.println("Route "+ route+ " has been removed from the routing table");

        if(remoteTaskExecutor._elasticTasks.get_routingTable().getRoutes().size()==0) {
            removeEmptyRemoteTaskExecutor(taskid);
        }



    }

    private void removeEmptyRemoteTaskExecutor(int taskid) {
        ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(taskid);
        remoteTaskExecutor.close();
        _originalTaskIdToRemoteTaskExecutor.remove(taskid);

        System.out.println("RemoteTaskExecutor " + taskid + " is interrupted!");
    }

    public void setworkerTopologyContext(WorkerTopologyContext context) {
        _workerTopologyContext = context;
    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        return _workerTopologyContext;
    }

    public double getThroughput(int taskid) {
        if(!_bolts.containsKey(taskid))
            return -1;
        return _bolts.get(taskid).getRate();
    }

    public String getDistribution(int taskid) {
        if(!_bolts.containsKey(taskid)) {
            return "task does not exist!";
        } else {
            return _bolts.get(taskid).get_elasticTasks()._sample.toString();
        }
    }

}
