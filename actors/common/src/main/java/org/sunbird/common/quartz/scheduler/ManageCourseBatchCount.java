/** */
package org.sunbird.common.quartz.scheduler;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.learner.actors.coursebatch.CourseEnrollmentActor;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This class will update course batch count in EKStep.
 *
 * @author Manzarul
 */
public class ManageCourseBatchCount implements Job {

  @SuppressWarnings("unchecked")
  public void execute(JobExecutionContext ctx) throws JobExecutionException {
    System.out.println("Working on this");
    ProjectLogger.log(
        "Executing COURSE_BATCH_COUNT job at: "
            + Calendar.getInstance().getTime()
            + " triggered by: "
            + ctx.getJobDetail().toString());
    Util.initializeContextForSchedulerJob(
        JsonKey.SYSTEM, ctx.getFireInstanceId(), JsonKey.SCHEDULER_JOB);
    Map<String, Object> logInfo =
        genarateLogInfo(JsonKey.SYSTEM, ctx.getJobDetail().getDescription());
    logInfo.put("LOG_LEVEL", "info");
    // Collect all those batches from ES whose start date is today and
    // countIncrementStatus value is
    // false.
    // and all those batches whose end date was yesterday and countDecrementStatus
    // value is false.
    // update the countIncrement or decrement status value as true , countIncrement
    // or decrement
    // date as today date.
    // make the status in course batch table based on course start or end - in case
    // of start make it
    // 1 , for end make it 2.
    // now update the data into cassandra and ES both and EKStep content with count
    // increment and
    // decrement value.
    SimpleDateFormat format = new SimpleDateFormat(ProjectUtil.YEAR_MONTH_DATE_FORMAT);
    Calendar cal = Calendar.getInstance();
    String today = "";
    String yesterDay = "";
    today = format.format(cal.getTime());
    cal.add(Calendar.DATE, -1);
    yesterDay = format.format(cal.getTime());
    ProjectLogger.log(
        "start date and end date is ==" + today + "  " + yesterDay, LoggerEnum.INFO.name());
    Map<String, Object> data = CourseBatchSchedulerUtil.getBatchDetailsFromES(today, yesterDay);
    Map<String, Map<String, Integer>> contentMap = new HashMap<>();
    Map<String, List<Map<String, Object>>> courseBatchMap = new HashMap<>();
    if (data != null && data.size() > 0) {
      if (null != data.get(JsonKey.START_DATE)) {
        List<Map<String, Object>> listMap =
            (List<Map<String, Object>>) data.get(JsonKey.START_DATE);
        for (Map<String, Object> map : listMap) {
          updateCourseBatchMap(true, false, map);
          updateContentAndCourseBatchMap(contentMap, courseBatchMap, map, true);
        }
      }
      if (null != data.get(JsonKey.END_DATE)) {
        List<Map<String, Object>> listMap = (List<Map<String, Object>>) data.get(JsonKey.END_DATE);
        for (Map<String, Object> map : listMap) {
          updateCourseBatchMap(false, true, map);
          updateContentAndCourseBatchMap(contentMap, courseBatchMap, map, false);
        }
      }
      if (null != data.get(JsonKey.STATUS)) {
        List<Map<String, Object>> listMap = (List<Map<String, Object>>) data.get(JsonKey.STATUS);
        for (Map<String, Object> map : listMap) {
          updateCourseBatchMap(false, false, map);
          boolean flag = CourseBatchSchedulerUtil.updateDataIntoES(map);
          if (flag) {
            CourseBatchSchedulerUtil.updateDataIntoCassandra(map);
          }
        }
      }
      updateEkstepAndDb(contentMap, courseBatchMap);
    } else {
      ProjectLogger.log(
          "No data found in Elasticsearch for course batch update.", LoggerEnum.INFO.name());
    }
    TelemetryUtil.telemetryProcessingCall(logInfo, null, null, "LOG");
  }

  private Map<String, Object> genarateLogInfo(String logType, String message) {

    Map<String, Object> info = new HashMap<>();
    info.put(JsonKey.LOG_TYPE, logType);
    long startTime = System.currentTimeMillis();
    info.put(JsonKey.START_TIME, startTime);
    info.put(JsonKey.MESSAGE, message);
    info.put(JsonKey.LOG_LEVEL, JsonKey.INFO);

    return info;
  }

  private void updateCourseBatchMap(
      boolean isCountIncrementStatus,
      boolean isCountDecrementStatus,
      Map<String, Object> courseBatchMap) {
    String todayDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    courseBatchMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.STARTED.getValue());
    if (isCountIncrementStatus) {
      courseBatchMap.put(JsonKey.COUNTER_INCREMENT_STATUS, true);
      courseBatchMap.put(JsonKey.COUNT_INCREMENT_DATE, todayDate);
    }
    if (isCountDecrementStatus) {
      courseBatchMap.put(JsonKey.COUNTER_DECREMENT_STATUS, true);
      courseBatchMap.put(JsonKey.COUNT_DECREMENT_DATE, todayDate);
      courseBatchMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.COMPLETED.getValue());
    }
    courseBatchMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    /*
    if(isCountIncrementStatus) {
     CourseBatchSchedulerUtil.updateCourseBatchDbStatus(weakMap, true);
    	}
    else if(isCountDecrementStatus) {
     CourseBatchSchedulerUtil.updateCourseBatchDbStatus(weakMap, false);
    }
    else {
     boolean flag = CourseBatchSchedulerUtil.updateDataIntoES(weakMap);
     if(flag) {
      CourseBatchSchedulerUtil.updateDataIntoCassandra(weakMap);
     }
    }
    */
  }

  private void updateContentAndCourseBatchMap(
      Map<String, Map<String, Integer>> contentMap,
      Map<String, List<Map<String, Object>>> courseBatchMap,
      Map<String, Object> map,
      boolean flag) {
    String name = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_INSTALLATION);
    String courseId = (String) map.get(JsonKey.COURSE_ID);
    String contentNameMatch = "", contentNameNotMatch = "";
    int batchCountMatchUpdatedVal, batchCountNotMatchVal;
    String enrollmentType = (String) map.get(JsonKey.ENROLLMENT_TYPE);
    if (enrollmentType.equals(ProjectUtil.EnrolmentType.open.getVal())) {
      contentNameMatch = "c_" + name + "_open_batch_count";
      contentNameNotMatch = "c_" + name + "_private_batch_count";
    } else {
      contentNameMatch = "c_" + name + "_private_batch_count";
      contentNameNotMatch = "c_" + name + "_private_batch_count";
    }
    if (!contentMap.containsKey(courseId)) {
      Map<String, String> ekstepHeader = CourseBatchSchedulerUtil.headerMap;
      Map<String, Object> ekStepContent =
          CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, ekstepHeader);
      if (ekStepContent != null && ekStepContent != null && ekStepContent.size() > 0) {
        //// Creating contentMap for this courseId
        Map<String, Integer> countMap = new HashMap<>();
        batchCountMatchUpdatedVal =
            CourseBatchSchedulerUtil.getUpdatedBatchCount(ekStepContent, contentNameMatch, flag);
        batchCountNotMatchVal = (int) ekStepContent.getOrDefault(contentNameNotMatch, 0);
        countMap.put(contentNameMatch, batchCountMatchUpdatedVal);
        countMap.put(contentNameNotMatch, batchCountNotMatchVal);
        contentMap.put(courseId, countMap);
        // Creating courseBatchMap for this courseId
        List<Map<String, Object>> courseBatchList = new ArrayList<>();
        courseBatchList.add(map);
        courseBatchMap.put(courseId, courseBatchList);
      }
    } else {
      batchCountMatchUpdatedVal = contentMap.get(courseId).get(contentNameMatch) + (flag ? 1 : -1);
      if (batchCountMatchUpdatedVal < 0) {
        batchCountMatchUpdatedVal = 0;
      }
      contentMap.get(courseId).put(contentNameMatch, batchCountMatchUpdatedVal);
      courseBatchMap.get(courseId).add(map);
    }
  }

  private void updateEkstepAndDb(
      Map<String, Map<String, Integer>> contentMap,
      Map<String, List<Map<String, Object>>> courseBatchMap) {
    boolean response;
    for (Map.Entry<String, Map<String, Integer>> ekStepUpdateMap : contentMap.entrySet()) {
      String courseId = ekStepUpdateMap.getKey();
      for (Map.Entry<String, Integer> batchTypeCount : ekStepUpdateMap.getValue().entrySet()) {
        response =
            CourseBatchSchedulerUtil.updateEkstepContent(
                courseId, batchTypeCount.getKey(), batchTypeCount.getValue());
        if (response) {
          List<Map<String, Object>> courseBatchUpdateList = courseBatchMap.get(courseId);
          if (courseBatchUpdateList != null && !courseBatchUpdateList.isEmpty()) {}
        }
      }
    }
  }
}
