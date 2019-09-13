package org.sunbird.middleware;

import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.common.ShadowUserProcessor;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.quartz.scheduler.ShadowUserMigrationScheduler;
import org.sunbird.learner.util.SchedulerManager;
import org.sunbird.learner.util.Util;

/** @author Mahesh Kumar Gangula */
public class Application {

  public static void main(String[] args) {
    SunbirdMWService.init();
     checkCassandraConnection();
////      ShadowUserProcessor object=new  ShadowUserProcessor();
////      object.process();
//      ShadowUserMigrationScheduler s=new ShadowUserMigrationScheduler();
//      s.processRecords();
  }

  public static void checkCassandraConnection() {
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD_PLUGIN);
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
