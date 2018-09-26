package org.sunbird.learner.actors.coursebatch.coursebatchvalidator;

import java.util.Map;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.user.courses.UserCourses;

public class Useruneroll {

  /*
   * This method will validate courseBatch details unenrolling
   *
   * @Params
   */
  public static void validateUserUneroll(UserCourses userCourseResult) {
    if (userCourseResult == null) {
      ProjectLogger.log(
          "CourseEnrollmentActor unenrollCourseClass user is not enrolled yet.",
          LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.userNotEnrolledCourse.getErrorCode(),
          ResponseCode.userNotEnrolledCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (!userCourseResult.isActive()) {
      ProjectLogger.log("User Have not Enrolled Course ");
      throw new ProjectCommonException(
          ResponseCode.userNotEnrolledCourse.getErrorCode(),
          ResponseCode.userNotEnrolledCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    // Check if user has completed the course
    if (userCourseResult.getProgress() == userCourseResult.getLeafNodesCount()) {
      ProjectLogger.log("User already have completed course ");
      throw new ProjectCommonException(
          ResponseCode.userAlreadyCompletedCourse.getErrorCode(),
          ResponseCode.userAlreadyCompletedCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }
  /**
   * This method will combined map values with delimiter and create an encrypted key.
   *
   * @param req Map<String , Object>
   * @return String encrypted value
   */
  public static String generateUserCoursesPrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId);
  }

  public static void updateUserCourseBatchToES(Map<String, Object> courseMap, String id) {
    boolean response =
        ElasticSearchUtil.updateData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.usercourses.getTypeName(),
            id,
            courseMap);
    ProjectLogger.log(
        "CourseEnrollmentActor updateUserCourseBatchToES user course unenrollment response "
            + response);
  }
}
