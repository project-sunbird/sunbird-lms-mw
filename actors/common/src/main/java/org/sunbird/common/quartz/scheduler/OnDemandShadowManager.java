package org.sunbird.common.quartz.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

public class OnDemandShadowManager {

  private static final String FILE = "quartz.properties";
  private static Scheduler scheduler = null;
  private static org.sunbird.common.quartz.scheduler.OnDemandShadowManager OnDemandShadowManager = null;

  private OnDemandShadowManager() {
   schedule(null);
  }

  /** This method will register the quartz scheduler job. */
  private void schedule(String jobs) {
   ProjectLogger.log(
           "OnDemandShadowManager:schedule: Call to start scheduler jobs - org.sunbird.common.quartz.scheduler.OnDemandShadowManager",
           LoggerEnum.INFO.name());

   try {
    boolean isEmbedded = false;
    Properties configProp = null;
    String embeddVal = System.getenv(JsonKey.SUNBIRD_QUARTZ_MODE);
    if (JsonKey.EMBEDDED.equalsIgnoreCase(embeddVal)) {
     isEmbedded = true;
    } else {
     configProp = setUpClusterMode();
    }
    if (!isEmbedded && configProp != null) {
     ProjectLogger.log("Quartz scheduler is running in cluster mode.", LoggerEnum.INFO.name());
     scheduler = new StdSchedulerFactory(configProp).getScheduler();
    } else {
     ProjectLogger.log("Quartz scheduler is running in embedded mode.", LoggerEnum.INFO.name());
     scheduler = new StdSchedulerFactory().getScheduler();
    }

   } catch (Exception e) {
    ProjectLogger.log(
            "OnDemandShadowManager:schedule: Error in starting scheduler jobs - org.sunbird.common.quartz.scheduler.OnDemandShadowManager "
                    + e.getMessage(),
            LoggerEnum.ERROR.name());
   } finally {
    registerShutDownHook();
   }
   ProjectLogger.log(
           "OnDemandShadowManager:schedule: started scheduler jobs - org.sunbird.common.quartz.scheduler.OnDemandShadowManager",
           LoggerEnum.INFO.name());
  }

  private void scheduleBulkUploadJobOnDemand(String identifier) {
   ProjectLogger.log(
           "OnDemandShadowManager:scheduleBulkUploadJobOnDemand:scheduler started",
           LoggerEnum.INFO.name());
   JobDetail uploadVerifyJob =
           JobBuilder.newJob(UploadLookUpScheduler.class)
                   .requestRecovery(true)
                   .withDescription("Scheduler for bulk upload retry")
                   .withIdentity("uploadVerifyScheduler", identifier)
                   .build();
   Trigger uploadTrigger =
           TriggerBuilder.newTrigger()
                   .withIdentity("uploadVerifyTrigger", identifier).startNow()
                   .withSchedule(simpleSchedule().withIntervalInSeconds(1).withRepeatCount(0))
                   .build();
   try {
    scheduler.scheduleJob(uploadVerifyJob, uploadTrigger);
    scheduler.start();
    ProjectLogger.log(
            "OnDemandShadowManager:scheduleBulkUploadJobOnDemand: UploadLookUpScheduler schedular started",
            LoggerEnum.INFO.name());
   } catch (Exception e) {
    ProjectLogger.log("OnDemandShadowManager:scheduleBulkUploadJobOnDemand Error occurred " + e.getMessage(),
            LoggerEnum.ERROR.name());
   }
  }

  private void scheduleShadowUserOnDemand(String identifier) {
   ProjectLogger.log(
           "OnDemandShadowManager:scheduleShadowUserOnDemand:scheduler started",
           LoggerEnum.INFO.name());
   JobDetail migrateShadowUserJob =
           JobBuilder.newJob(ShadowUserMigrationScheduler.class)
                   .requestRecovery(true)
                   .withDescription("Scheduler for migrating shadow user ")
                   .withIdentity("migrateShadowUserScheduler", identifier)
                   .build();
   Trigger migrateShadowUserTrigger =
           TriggerBuilder.newTrigger()
                   .withIdentity("migrateShadowUserTrigger", identifier).startNow()
                   .withSchedule(simpleSchedule().withIntervalInSeconds(1).withRepeatCount(0))
                   .build();
   try {
    if (scheduler.checkExists(migrateShadowUserJob.getKey())) {
     scheduler.deleteJob(migrateShadowUserJob.getKey());
    }
    scheduler.scheduleJob(migrateShadowUserJob, migrateShadowUserTrigger);
    scheduler.start();
    ProjectLogger.log(
            "OnDemandShadowManager:scheduleShadowUserOnDemand:scheduler ended",
            LoggerEnum.INFO.name());
   } catch (Exception e) {
    ProjectLogger.log(
            "OnDemandShadowManager:scheduleShadowUserOnDemand Error occurred " + e.getMessage(),
            LoggerEnum.ERROR.name());
   }
  }

  /**
   * This method will do the Quartz scheduler set up in cluster mode.
   *
   * @return Properties
   * @throws IOException
   */
  public Properties setUpClusterMode() throws IOException {
   Properties configProp = new Properties();
   InputStream in = this.getClass().getClassLoader().getResourceAsStream(FILE);
   String host = System.getenv(JsonKey.SUNBIRD_PG_HOST);
   String port = System.getenv(JsonKey.SUNBIRD_PG_PORT);
   String db = System.getenv(JsonKey.SUNBIRD_PG_DB);
   String username = System.getenv(JsonKey.SUNBIRD_PG_USER);
   String password = System.getenv(JsonKey.SUNBIRD_PG_PASSWORD);
   if (!StringUtils.isBlank(host)
           && !StringUtils.isBlank(port)
           && !StringUtils.isBlank(db)
           && !StringUtils.isBlank(username)
           && !StringUtils.isBlank(password)) {
    ProjectLogger.log(
            "Taking Postgres value from Environment variable...", LoggerEnum.INFO.name());
    configProp.load(in);
    configProp.put(
            "org.quartz.dataSource.MySqlDS.URL", "jdbc:postgresql://" + host + ":" + port + "/" + db);
    configProp.put("org.quartz.dataSource.MySqlDS.user", username);
    configProp.put("org.quartz.dataSource.MySqlDS.password", password);
    ProjectLogger.log(
            "OnDemandShadowManager:setUpClusterMode: Connection is established from environment variable",
            LoggerEnum.INFO);
   } else {
    ProjectLogger.log(
            "OnDemandShadowManager:setUpClusterMode: Environment variable is not set for postgres SQl.",
            LoggerEnum.INFO.name());
    configProp = null;
   }
   return configProp;
  }

  public static org.sunbird.common.quartz.scheduler.OnDemandShadowManager getInstance() {
   if (OnDemandShadowManager != null) {
    return OnDemandShadowManager;
   } else {
    OnDemandShadowManager = new org.sunbird.common.quartz.scheduler.OnDemandShadowManager();
   }
   return OnDemandShadowManager;
  }

  public void triggerScheduler(String[] jobs) {
   String identifier = "NetOps-PC1502295457753";
   for(String job: jobs) {
     switch (job) {
      case "bulkupload":
       scheduleBulkUploadJobOnDemand(identifier);
       break;
      case "shadowuser":
       scheduleShadowUserOnDemand(identifier);
       break;
      default:
       ProjectLogger.log(
               "OnDemandShadowManager:schedule: There is no such job",
               LoggerEnum.INFO.name());
     }
   }
  }

  /**
   * This class will be called by registerShutDownHook to register the call inside jvm , when jvm
   * terminate it will call the run method to clean up the resource.
   *
   * @author Manzarul
   */
  static class ResourceCleanUp extends Thread {
   @Override
   public void run() {
    ProjectLogger.log(
            "OnDemandShadowManager:ResourceCleanUp: started resource cleanup for Quartz job.",
            LoggerEnum.INFO);
    try {
     scheduler.shutdown();
    } catch (SchedulerException e) {
     ProjectLogger.log(e.getMessage(), e);
    }
    ProjectLogger.log(
            "OnDemandShadowManager:ResourceCleanUp: completed resource cleanup Quartz job.",
            LoggerEnum.INFO);
   }
  }

  /** Register the hook for resource clean up. this will be called when jvm shut down. */
  public static void registerShutDownHook() {
   Runtime runtime = Runtime.getRuntime();
   runtime.addShutdownHook(new org.sunbird.common.quartz.scheduler.OnDemandShadowManager.ResourceCleanUp());
   ProjectLogger.log(
           "OnDemandShadowManager:registerShutDownHook: ShutDownHook registered for Quartz scheduler.",
           LoggerEnum.INFO);
  }
}
