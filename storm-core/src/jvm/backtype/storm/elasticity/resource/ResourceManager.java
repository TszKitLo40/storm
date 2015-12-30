package backtype.storm.elasticity.resource;

import backtype.storm.elasticity.message.actormessage.WorkerCPULoad;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/22/15.
 */
public class ResourceManager {

    public Map<String, Double> workerCPULoad = new HashMap<>();
    public Map<String, Double> systemCPULoad = new HashMap<>();

    static ResourceManager instance;

    public static ResourceManager instance() {
        return instance;
    }

    public ResourceManager() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(1000);
//                        print();

                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
        instance = this;
    }

    public void updateWorkerCPULoad(String worker, WorkerCPULoad load) {
        workerCPULoad.put(worker, load.processCpuLoad);
        systemCPULoad.put(worker, load.systemCpuLoad);
    }

    void print() {
        NumberFormat formatter = new DecimalFormat("#0.0000");
        for(String worker: workerCPULoad.keySet()) {
            System.out.println(worker + ": " + formatter.format(workerCPULoad.get(worker)) + "\t" + formatter.format(systemCPULoad.get(worker)));
        }
    }

    public Map<String, Double> getWorkerCPULoadCopy() {
        Map<String, Double> ret = new HashMap<>();
        ret.putAll(workerCPULoad);
        return ret;
    }

}
