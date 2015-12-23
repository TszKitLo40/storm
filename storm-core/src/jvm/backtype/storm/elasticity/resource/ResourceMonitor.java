package backtype.storm.elasticity.resource;

import backtype.storm.elasticity.actors.Master;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * Created by robert on 12/22/15.
 */
public class ResourceMonitor {

    OperatingSystemMXBean operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();


    public ResourceMonitor() {
        createProcessCPULoadReportThread();
    }

    private void createProcessCPULoadReportThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(Config.ProcessCPULoadReportCycleInSecs * 1000);
                        double processCPULoad = getProcessCPULoad();
                        double systemCPULoad = getSystemCPULoad();
                        Slave.getInstance().reportWorkerCPULoadToMaster(processCPULoad, systemCPULoad);
                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private double getProcessCPULoad() {
        return operatingSystemMXBean.getProcessCpuLoad() * operatingSystemMXBean.getAvailableProcessors();
    }

    private double getSystemCPULoad() {
        return operatingSystemMXBean.getSystemCpuLoad() * operatingSystemMXBean.getAvailableProcessors();
    }
}
