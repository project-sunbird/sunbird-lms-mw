package org.sunbird.learner.util;

import java.util.Map;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;

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
}
