/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.sunbird.common.models.util.ProjectLogger;


/**
 * 
 * This class will manage all the Quartz scheduler.
 * We need to call the schedule method at one time.
 * we are calling this method from Util.java class.
 * @author Manzarul
 *
 */
public class SchedulerManager {
  
  private static final String file = "quartz.properties";
  private static Scheduler scheduler = null;
  private static SchedulerManager schedulerManager = null;
  
   private SchedulerManager() throws CloneNotSupportedException{
     schedule();
   }
  
  /**
   * This method will register the quartz scheduler job.
   */
  private  void schedule() {
    
    InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
    Properties configProp = new Properties();
    try {
      configProp.load(in);
       scheduler = new StdSchedulerFactory(configProp).getScheduler();
     String identifier = "NetOps-PC1502295457753";
   // 1- create a job and bind with class which is implementing Job
      // interface.
      JobDetail job = JobBuilder.newJob(ManageCourseBatchCount.class).requestRecovery(true).withIdentity("schedulerJob", identifier).build();
      
      // 2- Create a trigger object that will define frequency of run.
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity("schedulertrigger", identifier)
          .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(2).repeatForever()).build();
      try {
         if (scheduler.checkExists(job.getKey())){
          scheduler.deleteJob(job.getKey());
         }
          scheduler.scheduleJob(job, trigger);
          scheduler.start();
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      
      // add another job for verifying the bulk upload part.
      // 1- create a job and bind with class which is implementing Job
      // interface.
      JobDetail uploadVerifyJob = JobBuilder.newJob(UploadLookUpScheduler.class).requestRecovery(true).withIdentity("uploadVerifyScheduler", identifier).build();
      
      // 2- Create a trigger object that will define frequency of run.
      Trigger uploadTrigger = TriggerBuilder.newTrigger().withIdentity("uploadVerifyTrigger", identifier)
          .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(3).repeatForever()).build();
      try {
         if (scheduler.checkExists(uploadVerifyJob.getKey())){
          scheduler.deleteJob(uploadVerifyJob.getKey());
         }
          scheduler.scheduleJob(uploadVerifyJob, uploadTrigger);
          scheduler.start();
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      
      // add another job for verifying the course published details from EKStep.
      // 1- create a job and bind with class which is implementing Job
      // interface.
      JobDetail coursePublishedJob = JobBuilder.newJob(CoursePublishedUpdate.class).requestRecovery(true).withIdentity("coursePublishedScheduler", identifier).build();
      
      // 2- Create a trigger object that will define frequency of run.
      Trigger coursePublishedTrigger = TriggerBuilder.newTrigger().withIdentity("coursePublishedTrigger", identifier)
          .withSchedule(SimpleScheduleBuilder.repeatHourlyForever(1).repeatForever()).build();
      try {
         if (scheduler.checkExists(coursePublishedJob.getKey())){
          scheduler.deleteJob(coursePublishedJob.getKey());
         }
          scheduler.scheduleJob(coursePublishedJob, coursePublishedTrigger);
          scheduler.start();
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }  
      
      
    } catch (IOException | SchedulerException e ) {
      ProjectLogger.log("Error in properties cache", e);
    } finally {
        registerShutDownHook();
    }
  }
  
  
  public static SchedulerManager getInstance () {
     if(schedulerManager != null) {
       return schedulerManager;
     } 
     try {
       schedulerManager = new SchedulerManager();
    } catch (CloneNotSupportedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
     return schedulerManager;
  }
  
  public static void main(String[] args) {
    getInstance();
  }

  
  /**
   * This class will be called by registerShutDownHook to 
   * register the call inside jvm , when jvm terminate it will call
   * the run method to clean up the resource.
   * @author Manzarul
   *
   */
  static class ResourceCleanUp extends Thread {
        public void run() {
            ProjectLogger.log("started resource cleanup for Quartz job.");
            try {
              scheduler.shutdown();
            } catch (SchedulerException e) {
              ProjectLogger.log(e.getMessage(), e);
            }
            ProjectLogger.log("completed resource cleanup Quartz job.");
        }
  }
  
  /**
   * Register the hook for resource clean up.
   * this will be called when jvm shut down.
   */
  public static void registerShutDownHook() {
      Runtime runtime = Runtime.getRuntime();
      runtime.addShutdownHook(new ResourceCleanUp());
      ProjectLogger.log("ShutDownHook registered for Quartz scheduler.");
  }

  
  
}
