package backtype.storm.elasticity.common;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

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

    @Override
    public int hashCode() {
        return new Integer(shardId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ShardWorkload) {
            return ((ShardWorkload) obj).shardId == shardId;
        } else
            return false;
    }

    static public void main(String[] args) {
        Set<ShardWorkload> workloadSet = new HashSet<>();
        workloadSet.add(new ShardWorkload(1, 10000L));
        workloadSet.remove(new ShardWorkload(1, 200L));
        System.out.println(workloadSet.size());
    }

}
