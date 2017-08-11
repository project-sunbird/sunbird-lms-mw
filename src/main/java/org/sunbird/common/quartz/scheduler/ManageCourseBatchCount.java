/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;

/**
 * This class will update course batch count in EKStep.
 * @author Manzarul
 *
 */
public class ManageCourseBatchCount implements Job {
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    System.out.println("Executing at: " + Calendar.getInstance().getTime() + " triggered by: " + ctx.getJobDetail().toString());
    //Collect all those batches from ES whose start date is today and countIncrementStatus value is false.
    //or all those batches whose end date was yesterday and countDecrementStatus value is false.
    //update the countIncrement or decrement status value as true , countIncrement or decrement date as today date.
    //make the status based on course start or end - in case of start make it 1 , for end make it 2. 
    // now update the data into cassandra plus ES both and EKStep content with count increment and decrement value.
    SimpleDateFormat format = new SimpleDateFormat(ProjectUtil.YEAR_MONTH_DATE_FORMAT);
    Calendar cal = Calendar.getInstance();
    String today =""; String yesterDay = "";
     today = format.format(cal.getTime());
     cal.add(Calendar.DATE, -1);
      yesterDay = format.format(cal.getTime());
      ProjectLogger.log("start date and end date is ==" + today +"  "+ yesterDay);
    Map<String,Object> data =  CourseBatchSchedulerUtil.getBatchDetailsFromES(today, yesterDay);
    
    
  }
}
