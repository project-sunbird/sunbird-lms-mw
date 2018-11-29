/** */
package org.sunbird.learner.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.CourseEnrollmentActor;

/**
 * This class will update course batch count to EKStep. First it will get batch details from ES ,
 * then collect old open/private batch count value form EKStep then update cassandra db and EKStep
 * course instance count under EKStep.
 *
 * @author Manzarul
 */
public final class CourseBatchSchedulerUtil {
  public static Map<String, String> headerMap = new HashMap<>();

  static {
    String header = ProjectUtil.getConfigValue(JsonKey.EKSTEP_AUTHORIZATION);
    header = JsonKey.BEARER + header;
    headerMap.put(JsonKey.AUTHORIZATION, header);
    headerMap.put("Content-Type", "application/json");
  }

  private CourseBatchSchedulerUtil() {}

  /**
   * @param startDate
   * @param endDate
   * @return
   */
  public static Map<String, Object> getBatchDetailsFromES(String startDate, String endDate) {
    ProjectLogger.log(
        "method call start to collect get course batch data -" + startDate + " " + endDate,
        LoggerEnum.INFO.name());
    Map<String, Object> response = new HashMap<>();
    List<Map<String, Object>> courseBatchStartedList =
        courseBatchStartAndCompleteData(startDate, true);
    if (!courseBatchStartedList.isEmpty()) {
      response.put(JsonKey.START_DATE, courseBatchStartedList);
    }
    List<Map<String, Object>> courseBatchCompletedList =
        courseBatchStartAndCompleteData(endDate, false);
    if (!courseBatchCompletedList.isEmpty()) {
      response.put(JsonKey.END_DATE, courseBatchCompletedList);
    }
    List<Map<String, Object>> courseBatchStartStatusList = courseBatchStartStatusData(startDate);
    if (!courseBatchStartStatusList.isEmpty()) {
      response.put(JsonKey.STATUS, courseBatchStartStatusList);
    }
    ProjectLogger.log(
        "method call end to collect get course batch data -" + startDate + " " + endDate,
        LoggerEnum.INFO.name());
    return response;
  }
  /**
   * Method to update course batch status to db as well as EkStep .
   *
   * @param increment
   * @param map
   */
  public static void updateCourseBatchDbStatus(Map<String, Object> map, Boolean increment) {
    ProjectLogger.log("updating course batch details start", LoggerEnum.INFO.name());
    try {
      boolean response =
          doOperationInEkStepCourse(
              (String) map.get(JsonKey.COURSE_ID),
              increment,
              (String) map.get(JsonKey.ENROLLMENT_TYPE));
      ProjectLogger.log("Geeting response code back for update content == " + response);
      if (response) {
        boolean flag = updateDataIntoES(map);
        if (flag) {
          updateDataIntoCassandra(map);
        }
      } else {
        ProjectLogger.log("Ekstep content updatation failed.", LoggerEnum.INFO.name());
      }
    } catch (Exception e) {
      ProjectLogger.log("Exception occurred while savin data to course batch db ", e);
    }
  }

  /** @param map */
  public static boolean updateDataIntoES(Map<String, Object> map) {
    Boolean flag = true;
    try {
      flag =
          ElasticSearchUtil.updateData(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.course.getTypeName(),
              (String) map.get(JsonKey.ID),
              map);
    } catch (Exception e) {
      ProjectLogger.log("Exception occurred while saving course batch data to ES", e);
      flag = false;
    }
    return flag;
  }

  /** @param map */
  public static void updateDataIntoCassandra(Map<String, Object> map) {
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Util.DbInfo courseBatchDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    cassandraOperation.updateRecord(
        courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), map);
  }

  private static void addHeaderProps(Map<String, String> header, String key, String value) {
    header.put(key, value);
  }
  /**
   * Method to update the content state at ekstep : batch count
   *
   * @param courseId
   * @param increment
   * @param enrollmentType
   * @param hashTagId
   * @return
   */
  public static boolean doOperationInEkStepCourse(
      String courseId, boolean increment, String enrollmentType) {
    String contentName = getContentName(enrollmentType);
    boolean response = false;
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, getBasicHeader());
    if (ekStepContent != null && ekStepContent.size() > 0) {
      int val = getUpdatedBatchCount(ekStepContent, contentName, increment);
      if (ekStepContent.get(JsonKey.CHANNEL) != null) {
        ProjectLogger.log(
            "Channel value is coming from Content "
                + (String) ekStepContent.get(JsonKey.CHANNEL)
                + " Id "
                + courseId,
            LoggerEnum.INFO.name());
        addHeaderProps(
            getBasicHeader(), JsonKey.CHANNEL_ID, (String) ekStepContent.get(JsonKey.CHANNEL));
      } else {
        ProjectLogger.log(
            "Channel value is  not coming from Contnet Id " + courseId, LoggerEnum.INFO.name());
      }
      response = updateEkstepContent(courseId, contentName, val);
    } else {
      ProjectLogger.log(
          "EKstep content not found for course id==" + courseId, LoggerEnum.INFO.name());
    }
    return response;
  }

  private static Map<String, String> getBasicHeader() {
    return headerMap;
  }

  private static List<Map<String, Object>> courseBatchStartAndCompleteData(
      String date, boolean isStartDate) {
    String dateAttribute = isStartDate ? JsonKey.START_DATE : JsonKey.END_DATE;
    String counterAttribute =
        isStartDate ? JsonKey.COUNTER_INCREMENT_STATUS : JsonKey.COUNTER_DECREMENT_STATUS;
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<>();
    Map<String, String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put("<=", date);
    map.put(dateAttribute, dateRangeFilter);
    map.put(counterAttribute, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    String loggerMessageIfEmpty = "No data found for" + dateAttribute + " course batch===" + date;
    return contentReadResponse(dto, loggerMessageIfEmpty);
  }

  private static List<Map<String, Object>> courseBatchStartStatusData(String startDate) {
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<>();
    Map<String, String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put("<=", startDate);
    map.put(JsonKey.START_DATE, dateRangeFilter);
    map.put(JsonKey.COUNTER_INCREMENT_STATUS, true);
    map.put(JsonKey.STATUS, 0);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    String loggerMessageIfEmpty = "No data found for status change with start date " + startDate;
    return contentReadResponse(dto, loggerMessageIfEmpty);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> contentReadResponse(
      SearchDTO dto, String loggerMessageIfEmpty) {
    List<Map<String, Object>> listOfMap = new ArrayList<>();
    Map<String, Object> responseMap =
        ElasticSearchUtil.complexSearch(
            dto,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.CONTENT);
      if (val != null) {
        listOfMap = (List<Map<String, Object>>) val;
      } else {
        ProjectLogger.log(loggerMessageIfEmpty, LoggerEnum.INFO.name());
      }
    } else {
      ProjectLogger.log(loggerMessageIfEmpty, LoggerEnum.INFO.name());
    }
    return listOfMap;
  }

  public static String getContentName(String enrollmentType) {
    String name = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_INSTALLATION);
    String contentName = "";
    if (enrollmentType.equals(ProjectUtil.EnrolmentType.open.getVal())) {
      contentName = "c_" + name + "_open_batch_count";
    } else {
      contentName = "c_" + name + "_private_batch_count";
    }
    return contentName;
  }

  public static int getUpdatedBatchCount(
      Map<String, Object> ekStepContent, String contentName, boolean increment) {
    int val = (int) ekStepContent.getOrDefault(contentName, 0);
    if (increment) {
      val = val + 1;
    } else {
      if (val != 0) val = val - 1;
    }
    return val;
  }

  public static boolean updateEkstepContent(String courseId, String contentName, int val) {
    String response = "";
    try {
      ProjectLogger.log("updating content details to Ekstep start", LoggerEnum.INFO.name());
      String contentUpdateBaseUrl = ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL);
      response =
          HttpUtil.sendPatchRequest(
              contentUpdateBaseUrl
                  + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_UPDATE_URL)
                  + courseId,
              "{\"request\": {\"content\": {\"" + contentName + "\": " + val + "}}}",
              getBasicHeader());
      ProjectLogger.log(
          "batch count update response==" + response + " " + courseId, LoggerEnum.INFO.name());
    } catch (IOException e) {
      ProjectLogger.log("Error while updating content value " + e.getMessage(), e);
    }
    if (response.equalsIgnoreCase(JsonKey.SUCCESS)) return true;
    return false;
  }
}
