package org.sunbird.learner.actors;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EnrolmentType;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.CourseBatchDaoImpl;
import org.sunbird.learner.actors.usercourses.dao.UserCoursesDao;
import org.sunbird.learner.actors.usercourses.dao.impl.UserCoursesDaoImpl;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.CourseBatch;
import org.sunbird.models.user.courses.UserCourses;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Arvind
 */
@ActorConfig(
  tasks = {"enrollCourse", "unenrollCourse"},
  asyncTasks = {}
)
public class CourseEnrollmentActor extends BaseActor {

  private static String EKSTEP_COURSE_SEARCH_QUERY =
      "{\"request\": {\"filters\":{\"contentType\": [\"Course\"], \"objectType\": [\"Content\"], \"identifier\": \"COURSE_ID_PLACEHOLDER\", \"status\": \"Live\"},\"limit\": 1}}";

  private CourseBatchDao courseBatchDao = CourseBatchDaoImpl.getInstance();
  private UserCoursesDao userCourseDao = UserCoursesDaoImpl.getInstance();

  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param message Object (Request)
   */
  @Override
  public void onReceive(Request request) throws Throwable {

    ProjectLogger.log("CourseEnrollmentActor onReceive called");
    String operation = request.getOperation();

    Util.initializeContext(request, TelemetryEnvKey.BATCH);
    ExecutionContext.setRequestId(request.getRequestId());

    switch (operation) {
      case "enrollCourse":
        enrollCourseClass(request);
        break;
      case "unenrollCourse":
        unenrollCourseClass(request);
        break;
      default:
        onReceiveUnsupportedOperation("CourseEnrollmentActor");
    }
  }

  /**
   * Creates a enroll Course class for a user to enroll in a course.
   *
   * @param actorMessage Request message containing following request data: userId, courseId,
   *     BatchId
   * @return Return a promise for enroll course class API result.
   */
  private void enrollCourseClass(Request actorMessage) {
    ProjectLogger.log("enrollCourseClass called");

    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    Map<String, Object> request = actorMessage.getRequest();
    Map<String, Object> courseMap = (Map<String, Object>) actorMessage.getRequest();

    CourseBatch courseBatchResult = courseBatchDao.readById((String) request.get(JsonKey.BATCH_ID));
    validateCourseBatch(courseBatchResult, request);

    UserCourses userCourseResult = userCourseDao.read(generateUserCoursesPrimaryKey(courseMap));

    if (!ProjectUtil.isNull(userCourseResult) && userCourseResult.isActive()) {
      ProjectLogger.log("User Already Enrolled Course ");
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userAlreadyEnrolledCourse.getErrorCode(),
              ResponseCode.userAlreadyEnrolledCourse.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Timestamp ts = new Timestamp(new Date().getTime());
    String addedBy = (String) request.get(JsonKey.REQUESTED_BY);
    courseMap.put(
        JsonKey.COURSE_LOGO_URL, courseBatchResult.getCourseAdditionalInfo().get(JsonKey.APP_ICON));
    courseMap.put(JsonKey.CONTENT_ID, (String) request.get(JsonKey.COURSE_ID));
    courseMap.put(
        JsonKey.COURSE_NAME, courseBatchResult.getCourseAdditionalInfo().get(JsonKey.NAME));
    courseMap.put(
        JsonKey.DESCRIPTION, courseBatchResult.getCourseAdditionalInfo().get(JsonKey.DESCRIPTION));
    courseMap.put(JsonKey.ADDED_BY, addedBy);
    courseMap.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
    courseMap.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    courseMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    courseMap.put(JsonKey.DATE_TIME, ts);
    courseMap.put(JsonKey.ID, generateUserCoursesPrimaryKey(courseMap));
    courseMap.put(JsonKey.COURSE_PROGRESS, userCourseResult.getProgress());
    courseMap.put(
        JsonKey.LEAF_NODE_COUNT,
        courseBatchResult.getCourseAdditionalInfo().get(JsonKey.LEAF_NODE_COUNT));
    Response result = userCourseDao.insert(courseMap);
    sender().tell(result, self());
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) courseMap.get(JsonKey.USER_ID), JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.generateCorrelatedObject(
        (String) courseMap.get(JsonKey.COURSE_ID),
        JsonKey.COURSE,
        "user.batch.course",
        correlatedObject);
    TelemetryUtil.generateCorrelatedObject(
        (String) courseMap.get(JsonKey.BATCH_ID), JsonKey.BATCH, "user.batch", correlatedObject);

    TelemetryUtil.telemetryProcessingCall(request, targetObject, correlatedObject);
    // TODO: for some reason, ES indexing is failing with Timestamp value. need to
    // check and
    // correct it.
    courseMap.put(JsonKey.DATE_TIME, ProjectUtil.formatDate(ts));
    updateUserCoursesToES(courseMap);
    return;
  }

  /**
   * Creates a unenroll Course class for a user to enroll in a course.
   *
   * @param request Request message containing following request data: userId, courseId, BatchId
   * @return Return a promise for unenroll course class API result.
   */
  private void unenrollCourseClass(Request actorMessage) {
    ProjectLogger.log("unenrollCourseClass called");
    // objects of telemetry event...
    Map<String, Object> request = actorMessage.getRequest();
    CourseBatch courseBatchResult = courseBatchDao.readById((String) request.get(JsonKey.BATCH_ID));
    validateCourseBatch(courseBatchResult, request);
    UserCourses userCourseResult = userCourseDao.read(generateUserCoursesPrimaryKey(request));
    // check whether user already enroll for course
    if (userCourseResult == null) {
      ProjectLogger.log(
          "CourseEnrollmentActor unenrollCourseClass user is not enrolled yet.",
          LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotEnrolledCourse.getErrorCode(),
              ResponseCode.userNotEnrolledCourse.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    if (!userCourseResult.isActive()) {
      ProjectLogger.log("User Have not Enrolled Course ");
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotEnrolledCourse.getErrorCode(),
              ResponseCode.userNotEnrolledCourse.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    // Check if user has completed the course
    // TODO need to change course completed business logic
    if (userCourseResult.getProgress() == userCourseResult.getLeafNodesCount()) {
      ProjectLogger.log("User already have completed course ");
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userAlreadyCompletedCourse.getErrorCode(),
              ResponseCode.userAlreadyCompletedCourse.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String, Object> updateAttributes = new HashMap<String, Object>();
    updateAttributes.put(JsonKey.ACTIVE, false);
    updateAttributes.put(JsonKey.ID, userCourseResult.getId());
    Response result = userCourseDao.update(updateAttributes);
    sender().tell(result, self());
    updateUserCourseBatchToES(updateAttributes, userCourseResult.getId());
    generateAndProcessTelemetryEvent(request);
    return;
  }

  private void generateAndProcessTelemetryEvent(Map<String, Object> request) {
    Map<String, Object> targetObject = new HashMap<>();
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) request.get(JsonKey.USER_ID), JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.generateCorrelatedObject(
        (String) request.get(JsonKey.COURSE_ID),
        JsonKey.COURSE,
        "user.batch.course.unenroll",
        correlatedObject);
    TelemetryUtil.generateCorrelatedObject(
        (String) request.get(JsonKey.BATCH_ID), JsonKey.BATCH, "user.batch", correlatedObject);

    TelemetryUtil.telemetryProcessingCall(request, targetObject, correlatedObject);
  }

  private void updateUserCourseBatchToES(Map<String, Object> courseMap, String id) {
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

  private void insertUserCoursesToES(Map<String, Object> courseMap) {
    Request request = new Request();
    request.setOperation(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
    request.getRequest().put(JsonKey.USER_COURSES, courseMap);
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user count to Es : ", ex);
    }
  }

  private void updateUserCoursesToES(Map<String, Object> courseMap) {
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_USR_COURSES_INFO_ELASTIC.getValue());
    request.getRequest().put(JsonKey.USER_COURSES, courseMap);
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user count to Es : ", ex);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getCourseObjectFromEkStep(
      String courseId, Map<String, String> headers) {
    ProjectLogger.log("Requested course id is ==" + courseId, LoggerEnum.INFO.name());
    if (!StringUtils.isBlank(courseId)) {
      try {
        String query = EKSTEP_COURSE_SEARCH_QUERY.replaceAll("COURSE_ID_PLACEHOLDER", courseId);
        Map<String, Object> result = EkStepRequestUtil.searchContent(query, headers);
        if (null != result && !result.isEmpty()) {
          return ((List<Map<String, Object>>) result.get(JsonKey.CONTENTS)).get(0);
          //  return (Map<String, Object>) contentObject;
        }
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
    return null;
  }

  /**
   * This method will combined map values with delimiter and create an encrypted key.
   *
   * @param req Map<String , Object>
   * @return String encrypted value
   */
  private String generateUserCoursesPrimaryKey(Map<String, Object> req) {
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

  /**
   * This method will call the background job manager and update course enroll user count.
   *
   * @param operation String (operation name)
   * @param courseData Object
   * @param innerOperation String
   */
  @SuppressWarnings("unused")
  private void updateCoursemanagement(String operation, Object courseData, String innerOperation) {
    Request request = new Request();
    request.setOperation(operation);
    request.getRequest().put(JsonKey.COURSE_ID, courseData);
    request.getRequest().put(JsonKey.OPERATION, innerOperation);
    /*
     * userCountresponse.put(JsonKey.OPERATION, operation);
     * userCountresponse.put(JsonKey.COURSE_ID, courseData);
     * userCountresponse.getResult().put(JsonKey.OPERATION, innerOperation);
     */
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user count to Es : ", ex);
    }
  }

  /*
   * This method will validate courseBatch details before enrolling and unenrolling
   *
   * @Params
   */
  private void validateCourseBatch(CourseBatch courseBatchDetails, Map<String, Object> request) {

    if (ProjectUtil.isNull(courseBatchDetails)) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    if (EnrolmentType.inviteOnly.getVal().equals(courseBatchDetails.getEnrollmentType())) {
      ProjectLogger.log(
          "CourseEnrollmentActor validateCourseBatch self enrollment or unenrollment is not applicable for invite only batch.",
          LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.enrollmentTypeValidation.getErrorCode(),
              ResponseCode.enrollmentTypeValidation.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    if (!((String) request.get(JsonKey.COURSE_ID)).equals(courseBatchDetails.getCourseId())) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    try {
      Date todaydate = format.parse(format.format(new Date()));
      // there might be chance end date is not present
      Date courseBatchEndDate = null;
      if (StringUtils.isNotBlank(courseBatchDetails.getEndDate())) {
        courseBatchEndDate = format.parse(courseBatchDetails.getEndDate());
      }
      if (ProgressStatus.COMPLETED.getValue() == courseBatchDetails.getStatus()
          || (courseBatchEndDate != null && courseBatchEndDate.before(todaydate))) {
        ProjectLogger.log(
            "CourseEnrollmentActor validateCourseBatch Course is completed already.",
            LoggerEnum.INFO.name());
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.courseBatchAlreadyCompleted.getErrorCode(),
                ResponseCode.courseBatchAlreadyCompleted.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
    } catch (ParseException e) {
      ProjectLogger.log("CourseEnrollmentActor validateCourseBatch ", e);
    }
  }
}
