package backtype.storm.elasticity.common;

import java.util.Comparator;

/**
 * Created by robert on 1/27/16.
 */
public class SubTaskWorkload {
    public int subtaskId;
    public Long workload;
    public SubTaskWorkload(int subtaskId) {
        this(subtaskId, 0);
    }

    public SubTaskWorkload(int subtaskId, long workload) {
        this.subtaskId = subtaskId;
        this.workload = workload;
    }

    static public Comparator<SubTaskWorkload> createComparator() {
        return new Comparator<SubTaskWorkload>() {
            @Override
            public int compare(SubTaskWorkload subTaskWorkload, SubTaskWorkload subTaskWorkload2) {
                return Long.compare(subTaskWorkload.workload, subTaskWorkload2.workload);
            }
        };
    }


    static public Comparator<SubTaskWorkload> createReverseComparator() {
        return new Comparator<SubTaskWorkload>() {
            @Override
            public int compare(SubTaskWorkload subTaskWorkload, SubTaskWorkload subTaskWorkload2) {
                return - Long.compare(subTaskWorkload.workload, subTaskWorkload2.workload);
            }
        };
    }

    public void increaseOrDecraeseWorkload(long workload) {
        this.workload += workload;
    }
}
