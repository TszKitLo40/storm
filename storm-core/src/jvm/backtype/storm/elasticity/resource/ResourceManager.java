package backtype.storm.elasticity.resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/22/15.
 */
public class ResourceManager {

    Map<String, Double> workerCPULoad = new HashMap<>();

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
                        print();

                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
        instance = this;
    }

    public void updateWorkerCPULoad(String worker, double load) {
        workerCPULoad.put(worker, load);
    }

    void print() {
        for(String worker: workerCPULoad.keySet()) {
            System.out.println(worker + ": " + workerCPULoad.get(worker));
        }
    }
}
