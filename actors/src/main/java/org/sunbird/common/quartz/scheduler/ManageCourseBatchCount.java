/**
 * 
 */
package org.sunbird.common.quartz.scheduler;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;

/**
 * This class will update course batch count in EKStep.
 * 
 * @author Manzarul
 *
 */
public class ManageCourseBatchCount implements Job {
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    ProjectLogger.log("Executing at: " + Calendar.getInstance().getTime() + " triggered by: "
        + ctx.getJobDetail().toString());
    // Collect all those batches from ES whose start date is today and countIncrementStatus value is
    // false.
    // and all those batches whose end date was yesterday and countDecrementStatus value is false.
    // update the countIncrement or decrement status value as true , countIncrement or decrement
    // date as today date.
    // make the status in course batch table based on course start or end - in case of start make it
    // 1 , for end make it 2.
    // now update the data into cassandra and ES both and EKStep content with count increment and
    // decrement value.
    SimpleDateFormat format = new SimpleDateFormat(ProjectUtil.YEAR_MONTH_DATE_FORMAT);
    Calendar cal = Calendar.getInstance();
    String today = "";
    String yesterDay = "";
    today = format.format(cal.getTime());
    cal.add(Calendar.DATE, -1);
    yesterDay = format.format(cal.getTime());
    ProjectLogger.log("start date and end date is ==" + today + "  " + yesterDay,
        LoggerEnum.INFO.name());
    Map<String, Object> data = CourseBatchSchedulerUtil.getBatchDetailsFromES(today, yesterDay);
    if (data != null && data.size() > 0) {
      if (null != data.get(JsonKey.START_DATE)) {
        List<Map<String, Object>> listMap =
            (List<Map<String, Object>>) data.get(JsonKey.START_DATE);
        for (Map<String, Object> map : listMap) {
          Map<String, Object> weakMap = new WeakHashMap<>();
          weakMap.put(JsonKey.ID, (String) map.get(JsonKey.ID));
          weakMap.put(JsonKey.COURSE_ID, (String) map.get(JsonKey.COURSE_ID));
          weakMap.put(JsonKey.ENROLLMENT_TYPE, (String) map.get(JsonKey.ENROLLMENT_TYPE));
          weakMap.put(JsonKey.COUNTER_INCREMENT_STATUS, true);
          weakMap.put(JsonKey.COUNT_INCREMENT_DATE, ProjectUtil.getFormattedDate());
          weakMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.STARTED.getValue());
          weakMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
          CourseBatchSchedulerUtil.updateCourseBatchDbStatus(weakMap, true);
        }
      }
      if (null != data.get(JsonKey.END_DATE)) {
        List<Map<String, Object>> listMap = (List<Map<String, Object>>) data.get(JsonKey.END_DATE);
        for (Map<String, Object> map : listMap) {
          Map<String, Object> weakMap = new WeakHashMap<>();
          weakMap.put(JsonKey.ID, (String) map.get(JsonKey.ID));
          weakMap.put(JsonKey.ENROLLMENT_TYPE, (String) map.get(JsonKey.ENROLLMENT_TYPE));
          weakMap.put(JsonKey.COURSE_ID, (String) map.get(JsonKey.COURSE_ID));
          weakMap.put(JsonKey.COUNTER_DECREMENT_STATUS, true);
          weakMap.put(JsonKey.COUNT_DECREMENT_DATE, ProjectUtil.getFormattedDate());
          weakMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.COMPLETED.getValue());
          weakMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
          CourseBatchSchedulerUtil.updateCourseBatchDbStatus(weakMap, false);
        }
      }
    } else {
      ProjectLogger.log("No data found in Elasticsearch for course batch update.",
          LoggerEnum.INFO.name());
    }



  }
}
