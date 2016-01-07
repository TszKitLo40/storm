package backtype.storm.elasticity.config;

/**
 * Created by robert on 12/22/15.
 */
public class Config {

    public static int NumberOfShard = 256;

    public static int SubtaskInputQueueCapacity = 64;

    public static double RoutingSamplingRate = 1.0;

    public static int ResultQueueCapacity = 128;

    public static int RemoteExecutorInputQueueCapacity = 128;

    public static int StateCheckPointingCycleInSecs = 10;

    public static int ElasticTaskHolderOutputQueueCapacity = 32;

    public static int CreateBalancedHashRoutingSamplingTimeInSecs = 3;

    public static int ProcessCPULoadReportCycleInSecs = 1;

    public static int WorkloadHighWaterMark = 6;

    public static int WorkloadLowWaterMark = 3;

    public static int WorkerLevelLoadBalancingCycleInSecs = 10;

    public static int SubtaskLevelLoadBalancingCycleInSecs = 15;

    public static boolean EnableWorkerLevelLoadBalancing = true;

    public static boolean EnableSubtaskLevelLoadBalancing = false;

    public static int LoggingServerPort = 10000;

}
