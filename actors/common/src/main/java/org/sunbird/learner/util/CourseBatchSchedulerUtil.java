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
  @SuppressWarnings("unchecked")
  public static Map<String, Object> getBatchDetailsFromES(String startDate, String endDate) {
    ProjectLogger.log(
        "method call start to collect get course batch data -" + startDate + " " + endDate,
        LoggerEnum.INFO.name());
    Map<String, Object> response = new HashMap<>();
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<>();
    Map<String, String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put("<=", startDate);
    map.put(JsonKey.START_DATE, dateRangeFilter);
    map.put(JsonKey.COUNTER_INCREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    List<Map<String, Object>> listOfMap = new ArrayList<>();
    List<Map<String, Object>> endBatchMap = new ArrayList<>();
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
        ProjectLogger.log(
            "No data found for start date course batch===" + startDate, LoggerEnum.INFO.name());
      }
    } else {
      ProjectLogger.log(
          "No data found for start date course batch===" + startDate, LoggerEnum.INFO.name());
    }
    response.put(JsonKey.START_DATE, listOfMap);
    map.clear();
    dateRangeFilter.clear();
    dateRangeFilter.put("<=", endDate);
    map.put(JsonKey.END_DATE, dateRangeFilter);
    map.put(JsonKey.COUNTER_DECREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    responseMap =
        ElasticSearchUtil.complexSearch(
            dto,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.CONTENT);
      if (val != null) {
        endBatchMap = (List<Map<String, Object>>) val;
      } else {
        ProjectLogger.log(
            "No data found for end date course batch===" + endDate, LoggerEnum.INFO.name());
      }
    } else {
      ProjectLogger.log(
          "No data found for end date course batch===" + endDate, LoggerEnum.INFO.name());
    }
    response.put(JsonKey.END_DATE, endBatchMap);
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
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Util.DbInfo courseBatchDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
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
          cassandraOperation.updateRecord(
              courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), map);
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
    String name = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_INSTALLATION);
    String contentName = "";
    String response = "";
    if (enrollmentType.equals(ProjectUtil.EnrolmentType.open.getVal())) {
      contentName = "c_" + name + "_open_batch_count";
    } else {
      contentName = "c_" + name + "_private_batch_count";
    }
    Map<String, String> ekstepHeader = getBasicHeader();
    // addHeaderProps(ekstepHeader, JsonKey.CHANNEL_ID , hashTagId);
    // collect data from EKStep.
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, ekstepHeader);
    if (ekStepContent != null && ekStepContent.size() > 0) {
      int val = (int) ekStepContent.getOrDefault(contentName, 0);
      if (increment) {
        val = val + 1;
      } else {
        if (val != 0) val = val - 1;
      }
      if (ekStepContent.get(JsonKey.CHANNEL) != null) {
        ProjectLogger.log(
            "Channel value is coming from Contnet "
                + (String) ekStepContent.get(JsonKey.CHANNEL)
                + " Id "
                + courseId,
            LoggerEnum.INFO.name());
        addHeaderProps(
            ekstepHeader, JsonKey.CHANNEL_ID, (String) ekStepContent.get(JsonKey.CHANNEL));
      } else {
        ProjectLogger.log(
            "Channel value is  not coming from Contnet Id " + courseId, LoggerEnum.INFO.name());
      }

      try {
        ProjectLogger.log("updating content details to Ekstep start", LoggerEnum.INFO.name());
        String contentUpdateBaseUrl = ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL);
        response =
            HttpUtil.sendPatchRequest(
                contentUpdateBaseUrl
                    + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_UPDATE_URL)
                    + courseId,
                "{\"request\": {\"content\": {\"" + contentName + "\": " + val + "}}}",
                ekstepHeader);
        ProjectLogger.log(
            "batch count update response==" + response + " " + courseId, LoggerEnum.INFO.name());
      } catch (IOException e) {
        ProjectLogger.log("Error while updating content value " + e.getMessage(), e);
      }
    } else {
      ProjectLogger.log(
          "EKstep content not found for course id==" + courseId, LoggerEnum.INFO.name());
    }
    if (response.equalsIgnoreCase(JsonKey.SUCCESS)) return true;
    return false;
  }

  private static Map<String, String> getBasicHeader() {
    return headerMap;
  }
}
