/** */
package org.sunbird.learner.util;

/** @author Manzarul All the scheduler job will be handle by this class. */
public class SchedulerManager {

 private static final int PAGE_DATA_TTL = 24;

 /*
  * service ScheduledExecutorService object
  */
 public static ScheduledExecutorService service = ExecutorManager.getExecutorService();

 /** all scheduler job will be configure here. */
 public static void schedule() {
   service.scheduleWithFixedDelay(new DataCacheHandler(), 0, PAGE_DATA_TTL, TimeUnit.HOURS);
   ProjectLogger.log("started scheduler job - org.sunbird.learner.util.SchedulerManager");
 }
}
