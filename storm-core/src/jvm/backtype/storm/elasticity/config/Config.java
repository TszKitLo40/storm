package backtype.storm.elasticity.config;

/**
 * Created by robert on 12/22/15.
 */
public class Config {

    public static int NumberOfShard = 32;

    public static int SubtaskInputQueueCapacity = 64;

    public static double RoutingSamplingRate = 1.0;

    public static int ResultQueueCapacity = 128;

    public static int RemoteExecutorInputQueueCapacity = 128;

    public static int StateCheckPointingCycleInSecs = 10;

    public static int ElasticTaskHolderOutputQueueCapacity = 256;

    public static int CreateBalancedHashRoutingSamplingTimeInSecs = 3;

}
