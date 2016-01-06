package backtype.storm.elasticity.scheduler;

import backtype.storm.elasticity.actors.Master;
import backtype.storm.elasticity.common.RouteId;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.resource.ResourceManager;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.utils.FirstFitDoubleDecreasing;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.generated.TaskNotExistException;
import org.apache.thrift.TException;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.*;

/**
 * Created by Robert on 11/11/15.
 */
public class ElasticScheduler {

    Master master;

    ResourceManager resourceManager;

    final Object lock = new Object();

    static private ElasticScheduler instance;

    public ElasticScheduler() {
        master = Master.createActor();
        resourceManager = new ResourceManager();
        instance = this;

        if(Config.EnableWorkerLevelLoadBalancing) {
            enableWorkerLevelLoadBalancing();
        }

        if(Config.EnableSubtaskLevelLoadBalancing) {
            enableSubtaskLevelLoadBalancing();
        }
    }

    static public ElasticScheduler getInstance() {
        return instance;
    }

    private void enableWorkerLevelLoadBalancing() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(Config.WorkerLevelLoadBalancingCycleInSecs * 1000);
                        synchronized (lock) {
                            Set<Integer> taskIds = master._elasticTaskIdToWorker.keySet();
                            for(Integer task: taskIds) {
                                try {
//                                    workerLevelLoadBalancing(task);
                                } catch (TException e ) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void enableSubtaskLevelLoadBalancing() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(Config.SubtaskLevelLoadBalancingCycleInSecs * 1000);
                        synchronized (lock) {
                            Set<Integer> taskIds = master._elasticTaskIdToWorker.keySet();
                            for(Integer task: taskIds) {
                                try {
//                                    optimizeBucketToRoutingMapping(task);
                                } catch (Exception e ) {s
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public String optimizeBucketToRoutingMapping(int taskId) throws TaskNotExistException, RoutingTypeNotSupportedException, TException {

        synchronized (lock) {

            // 1. get routingTable
            RoutingTable routingTable = master.getRoutingTable(taskId);
            BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
            if(balancedHashRouting == null) {
                throw new RoutingTypeNotSupportedException("Only support BalancedHashRouting family routing table!");
            }
            System.out.println("routing Table: " + balancedHashRouting.toString());



            // 2. get Distribution;
            Histograms histograms = master.getBucketDistribution(taskId);
            System.out.println("Histograms: " + histograms.toString());

            // 3. evaluate the skewness
            Map<Integer, Integer> shardToRouteMapping = balancedHashRouting.getBucketToRouteMapping();
            final int numberOfRoutes = balancedHashRouting.getNumberOfRoutes();
            long[] routeLoads = new long[numberOfRoutes];

            for(Integer shard: shardToRouteMapping.keySet()) {
    //            System.out.println("\n\n shard:" + shard);
    //            System.out.println("numberOfRoutes: " + numberOfRoutes);
    //            System.out.println("shardToRouteMapping.get(shard): "+ shardToRouteMapping.get(shard) + "\n");
    //            System.out.println("routeLoads[shardToRouteMapping.get(shard)]" + routeLoads[shardToRouteMapping.get(shard)] + "\n");
    //            System.out.println("histograms.histogramsToArrayList().get(shard)" + histograms.histogramsToArrayList().get(shard) + "\n");
                routeLoads[shardToRouteMapping.get(shard)] += histograms.histogramsToArrayList().get(shard);
            }

            long loadSum = 0;
            long loadMin = Long.MAX_VALUE;
            long loadMax = Long.MIN_VALUE;
            for(Long i: routeLoads) {
                loadSum += i;
            }
            for(Long i: routeLoads) {
                if(loadMin > i){
                    loadMin = i;
                }
            }
            for(Long i: routeLoads) {
                if(loadMax < i) {
                    loadMax = i;
                }
            }

            double averageLoad = loadSum / (double)numberOfRoutes;
            boolean skewness = (loadMax - averageLoad)/averageLoad > 0.8;

            System.out.println("Workload distribution:\n");
            for(int i = 0; i < routeLoads.length; i++ ){
                System.out.println(i + ": " + routeLoads[i]);
            }
            System.out.println("Workload Skewness: " + skewness);

            if(skewness) {

                FirstFitDoubleDecreasing binPackingSolver = new FirstFitDoubleDecreasing(histograms.histogramsToArrayList(), numberOfRoutes);
                if(binPackingSolver.getResult() != numberOfRoutes) {
                    System.out.println("Fail to solve the bin packing problem!");
                    return null;
                }
                System.out.println(binPackingSolver.toString());


                Map<Integer, Integer> oldMapping = balancedHashRouting.getBucketToRouteMapping();
                Map<Integer, Integer> newMapping = binPackingSolver.getBucketToPartitionMap();
                ShardReassignmentPlan plan = new ShardReassignmentPlan();

                for(Integer bucket: oldMapping.keySet()) {
                    if(!oldMapping.get(bucket).equals(newMapping.get(bucket)) ) {
                        int oldRoute = oldMapping.get(bucket);
                        int newRoute = newMapping.get(bucket);
                        plan.addReassignment(taskId, bucket, oldRoute, newRoute);
                        System.out.println("Move " + bucket + " from " + oldRoute + " to " + newRoute + "\n");
                    }
                }

                if(!plan.getReassignmentList().isEmpty()) {
                    applyShardToRouteReassignment(plan);
                } else {
                    System.out.println("Shard Assignment is not modified after optimization.");
                }
                return plan.toString();
            } else {
                return "The workload is not skewed!";
            }
        }

    }

    void applyShardToRouteReassignment(ShardReassignmentPlan plan) throws TException{
        int totalMovements = plan.getReassignmentList().size();
        int i = 0;
        for(ShardReassignment reassignment: plan.getReassignmentList()) {
            System.out.println("Begin to conduct the " + i++ + "th movements, " + totalMovements + " in total!");
            master.reassignBucketToRoute(reassignment.taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
        }
    }

    void applySubtaskReassignmentPlan(SubtaskReassignmentPlan plan) throws TException {
        int totalMovements = plan.getSubTaskReassignments().size();
        int i = 0;
        for(SubtaskReassignment reassignment: plan.getSubTaskReassignments()) {
            System.out.println("Begin to conduct the " + i++ + "th movements, " + totalMovements + " in total!");
            master.migrateTasks(reassignment.originalHost, reassignment.targetHost, reassignment.taskId, reassignment.routeId);
        }
    }

    public String naiveWorkerLevelLoadBalancing(int taskId) throws TException {
        SubtaskReassignmentPlan plan = new SubtaskReassignmentPlan();

        synchronized (lock) {
            ArrayList<String> workers = new ArrayList<>();
            workers.addAll(resourceManager.systemCPULoad.keySet());
            int workerIndex = 0;
            Map<String, String> taskIdRouteToWorkers = master._taskidRouteToWorker;
            for(String xdy: taskIdRouteToWorkers.keySet()) {
                if(!taskIdRouteToWorkers.get(xdy).equals(workers.get(workerIndex))) {
                    RouteId routeId = new RouteId(xdy);
                    plan.addSubtaskReassignment(taskIdRouteToWorkers.get(xdy), workers.get(workerIndex), routeId.TaskId, routeId.Route);
                }
                workerIndex = (workerIndex + 1) % workers.size();
            }
            applySubtaskReassignmentPlan(plan);
        }

        return plan.toString();
    }

    public String workerLevelLoadBalancing(int taskId) throws TException {
        if(!master._elasticTaskIdToWorker.containsKey(taskId))
            throw new TaskNotExistException("Task " + taskId + " does not exists!");

        Map<String, Double> workerLoad = resourceManager.getWorkerCPULoadCopy();
        Map<String, String> taskIdRouteToWorkers = master._taskidRouteToWorker;

        SubtaskReassignmentPlan totalPlan = new SubtaskReassignmentPlan();

        synchronized (lock) {
            for(String worker: workerLoad.keySet()) {
                if(workerLoad.get(worker) >= Config.WorkloadHighWaterMark) {
                    SubtaskReassignmentPlan localPlan = releaseLoadOnWorker(taskId, worker, workerLoad, taskIdRouteToWorkers);
                    totalPlan.concat(localPlan);
    //                totalPlan.con
                }
            }

            System.out.println("Load balancing plan: " + totalPlan);
            applySubtaskReassignmentPlan(totalPlan);


            Map<String, ArrayList<String>> workerToSubtaskRoute = new HashMap<>();
            for(String xdy: taskIdRouteToWorkers.keySet()) {
                String worker = taskIdRouteToWorkers.get(xdy);
                if(!workerToSubtaskRoute.containsKey(worker)) {
                    workerToSubtaskRoute.put(worker, new ArrayList<String>());
                }
                workerToSubtaskRoute.get(worker).add(xdy);
            }
            System.out.println("Existing assignment:");
            for(String worker: workerToSubtaskRoute.keySet()) {
                String str = "";
                str += worker + ": ";
                for(String xdy: workerToSubtaskRoute.get(worker)) {
                    str += xdy + " ";
                }
                System.out.println(str);
            }
        }
        return totalPlan.toString();
    }

    SubtaskReassignmentPlan releaseLoadOnWorker(int taskId, String worker, Map<String, Double> workerLoad, Map<String, String> taskIdRouteToWorkers) {

        SubtaskReassignmentPlan plan = new SubtaskReassignmentPlan();

        String hostWorker = master._elasticTaskIdToWorker.get(taskId);

        Map<String, Queue<String>> workerToTaskRoutes = new HashMap<>();

        for(String taskIdRoute: taskIdRouteToWorkers.keySet()) {
            String w = taskIdRouteToWorkers.get(taskIdRoute);
            if(!workerToTaskRoutes.containsKey(w)) {
                workerToTaskRoutes.put(w, new ArrayQueue<String>());
            }
            workerToTaskRoutes.get(w).add(taskIdRoute);
        }

        int movements = (int) Math.ceil(workerLoad.get(worker) - Config.WorkloadHighWaterMark);


        //first try to move subtask to the original host
        if(!hostWorker.equals(worker) && workerLoad.get(hostWorker)<=Config.WorkloadLowWaterMark) {
            while(workerLoad.get(worker) > Config.WorkloadHighWaterMark // target worker is overloaded
                    && workerLoad.get(hostWorker) + 1 <= Config.WorkloadHighWaterMark // hostWorker is idle
                    && workerToTaskRoutes.get(worker).size() > 0 //there are subtasks on the target worker
                    ) {
                String taskIdRoute = workerToTaskRoutes.get(worker).poll();
                int tid = Integer.parseInt(taskIdRoute.split(".")[0]);
                int route = Integer.parseInt(taskIdRoute.split(".")[1]);
                assert(tid == taskId);
                plan.addSubtaskReassignment(worker, hostWorker, tid, route);
                workerLoad.put(worker, workerLoad.get(worker) - 1);
                workerLoad.put(hostWorker, workerLoad.get(hostWorker) + 1);
            }

        }

        for(String w: workerLoad.keySet()) {
            while(workerLoad.get(worker) > Config.WorkloadHighWaterMark
                    && workerLoad.get(w) + 1 <= Config.WorkloadHighWaterMark
                    && workerToTaskRoutes.get(worker).size() > 0
                    ) {
                String taskIdRoute = workerToTaskRoutes.get(worker).poll();
                System.out.println("taskIdRoute: " + taskIdRoute);
                int tid = Integer.parseInt(taskIdRoute.split("\\.")[0]);
                int route = Integer.parseInt(taskIdRoute.split("\\.")[1]);
                assert(tid == taskId);
                plan.addSubtaskReassignment(worker, w, tid, route);
                workerLoad.put(worker, workerLoad.get(worker) - 1);
                workerLoad.put(w, workerLoad.get(w) + 1);
            }
        }

        return plan;
    }

}
