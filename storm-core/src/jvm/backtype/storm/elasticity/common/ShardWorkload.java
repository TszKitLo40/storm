package backtype.storm.elasticity.common;

import java.util.Comparator;

/**
 * Created by robert on 1/27/16.
 */
public class ShardWorkload {
    public int shardId;
    public long workload;
    public ShardWorkload(int shardId, long workload) {
        this.shardId = shardId;
        this.workload = workload;
    }
    public ShardWorkload(int shardId) {
        this(shardId, 0);
    }

    static public Comparator<ShardWorkload> createComparator() {
        return new Comparator<ShardWorkload>() {
            @Override
            public int compare(ShardWorkload subTaskWorkload, ShardWorkload subTaskWorkload2) {
                return Long.compare(subTaskWorkload.workload, subTaskWorkload2.workload);
            }
        };
    }

    static public Comparator<ShardWorkload> createReverseComparator() {
        return new Comparator<ShardWorkload>() {
            @Override
            public int compare(ShardWorkload subTaskWorkload, ShardWorkload subTaskWorkload2) {
                return - Long.compare(subTaskWorkload.workload, subTaskWorkload2.workload);
            }
        };
    }
}
