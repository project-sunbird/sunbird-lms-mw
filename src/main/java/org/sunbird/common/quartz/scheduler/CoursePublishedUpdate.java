/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import java.util.Calendar;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * This class will call the EKstep get content api to 
 * know the status of course published. 
 * once course status becomes live then it will
 * update status under course published table and collect 
 * all participant  from course-batch table and register all those 
 * participant under user_course table and push the data to ES.
 * @author Manzarul
 *
 */
public class CoursePublishedUpdate implements Job {
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    System.out.println("Running Course published Scheduler Job at: " + Calendar.getInstance().getTime() + " triggered by: " + ctx.getJobDetail().toString());
  }
}
