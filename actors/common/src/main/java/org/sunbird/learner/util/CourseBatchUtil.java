package org.sunbird.learner.util;

import java.io.IOException;
import java.util.Map;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.models.course.batch.CourseBatch;

public class CourseBatchUtil {

  private CourseBatchUtil() {}

  public static void syncCourseBatchForeground(String uniqueId, Map<String, Object> req) {
    ProjectLogger.log(
        "CourseBatchManagementActor: syncCourseBatchForeground called for course batch ID = "
            + uniqueId,
        LoggerEnum.INFO.name());
    String esResponse =
        ElasticSearchUtil.createData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            uniqueId,
            req);
    ProjectLogger.log(
        "CourseBatchManagementActor::syncCourseBatchForeground: Sync response for course batch ID = "
            + uniqueId
            + " received response = "
            + esResponse,
        LoggerEnum.INFO.name());
  }

  public static String doOperationInEkStepCourse(
      Map<String, Object> ekStepContent, CourseBatch courseBatch, boolean increment) {
    String name = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_INSTALLATION);
    String contentName = "";
    String response = "";
    Map<String, String> ekstepHeader = CourseBatchSchedulerUtil.headerMap;
    if (courseBatch.getEnrollmentType().equals(ProjectUtil.EnrolmentType.open.getVal())) {
      contentName = "c_" + name + "_open_batch_count";
    } else {
      contentName = "c_" + name + "_private_batch_count";
    }
    int val = (int) ekStepContent.getOrDefault(contentName, 0);
    if (increment) {
      val = val + 1;
    } else {
      if (val != 0) val = val - 1;
    }
    try {
      ProjectLogger.log("updating content details to Ekstep start", LoggerEnum.INFO.name());
      String contentUpdateBaseUrl = ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL);
      response =
          HttpUtil.sendPatchRequest(
              contentUpdateBaseUrl
                  + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_UPDATE_URL)
                  + courseBatch.getCourseId(),
              "{\"request\": {\"content\": {\"" + contentName + "\": " + val + "}}}",
              ekstepHeader);
      ProjectLogger.log(
          "batch count update response==" + response + " " + courseBatch.getCourseId(),
          LoggerEnum.INFO.name());
    } catch (IOException e) {
      ProjectLogger.log("Error while updating content value " + e.getMessage(), e);
    }
    return response;
  }
}
