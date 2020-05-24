package org.sunbird.middleware;

import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.learner.util.SchedulerManager;
import org.sunbird.learner.util.Util;

/** @author Mahesh Kumar Gangula */
public class Application {

  public static void main(String[] args) {
    SunbirdMWService.init();
     init();
  }

  public static void init() {
    SchedulerManager.schedule();
    new Thread(
            new Runnable() {
              @Override
              public void run() {
                org.sunbird.common.quartz.scheduler.SchedulerManager.getInstance();
              }
            })
        .start();
  }
}
