package storm.starter;

/**
 * Created by acelzj on 21/06/16.
 */

import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;
import backtype.storm.utils.Utils;


public class TestRateTracker {
  public static void main(String[] args){
    final ThroughputMonitor monitor = new ThroughputMonitor("monitor");
    Thread notifyThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          Utils.sleep(2);
          monitor.rateTracker.notify(1);
        }
      }
    });
    notifyThread.start();
    Thread reportRateThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          Utils.sleep(1000);
          System.out.println(monitor.rateTracker.reportRate());
        }
      }
    });
   reportRateThread.start();
  }
}
