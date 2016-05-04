package backtype.storm.elasticity.config;

/**
 * Created by robert on 12/22/15.
 */
public class Config {

    public static int NumberOfShard = 64;

    public static double RoutingSamplingRate = 1.0;

    public static int SubtaskInputQueueCapacity = 8;

    public static int ResultQueueCapacity = 1024;

    public static int RemoteExecutorInputQueueCapacity = 8;

    public static int ElasticTaskHolderOutputQueueCapacity = 1024;

    public static int StateCheckPointingCycleInSecs = 10;

    public static int CreateBalancedHashRoutingSamplingTimeInSecs = 3;

    public static int ProcessCPULoadReportCycleInSecs = 1;

    public static int WorkloadHighWaterMark = 6;

    public static int WorkloadLowWaterMark = 3;

    public static int WorkerLevelLoadBalancingCycleInSecs = 10;

    public static int SubtaskLevelLoadBalancingCycleInSecs = 5;

    public static boolean EnableWorkerLevelLoadBalancing = false;

    public static boolean EnableSubtaskLevelLoadBalancing = true;

    public static boolean EnableAutomaticScaling = true;

    public static int LoggingServerPort = 10000;

    public static double latencySampleRate = 0.1;

    public static int numberOfLatencyHistoryRecords = 100;

    public static int latencyMaximalTimeIntervalInSecond = 1;

    public static double taskLevelLoadBalancingThreshold = 0.2;

    public static String masterIp = "192.168.0.183";

}
