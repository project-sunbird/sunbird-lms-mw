/**
 *
 */
package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import akka.pattern.Patterns;
import akka.util.Timeout;

import java.sql.Timestamp;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;

import scala.concurrent.duration.Duration;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Arvind
 */
public class CourseEnrollmentActor extends UntypedAbstractActor {

  private static String EKSTEP_COURSE_SEARCH_QUERY = "{\"request\": {\"filters\":{\"contentType\": [\"Course\"], \"objectType\": [\"Content\"], \"identifier\": \"COURSE_ID_PLACEHOLDER\", \"status\": \"Live\"},\"limit\": 1}}";
  private CassandraOperation cassandraOperation = new CassandraOperationImpl();

  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param message Object (Request)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("CourseEnrollmentActor  onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ENROLL_COURSE.getValue())) {
          Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
          Map<String, Object> req = actorMessage.getRequest();
          String addedBy = (String) req.get(JsonKey.REQUESTED_BY);
          Map<String, Object> courseMap = (Map<String, Object>) req.get(JsonKey.COURSE);
          //check whether user already enroll  for course
          Response dbResult = cassandraOperation.getRecordById(courseEnrollmentdbInfo.getKeySpace(),
              courseEnrollmentdbInfo.getTableName() , generatePrimaryKey(courseMap));
          List<Map<String , Object>> dbList = (List<Map<String, Object>>) dbResult.get(JsonKey.RESPONSE);
          if(!dbList.isEmpty()){
            ProjectLogger.log("User Already Enrolled Course ");
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.userAlreadyEnrolledThisCourse.getErrorCode(),
                ResponseCode.userAlreadyEnrolledThisCourse.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }

          Map<String, String> headers = (Map<String, String>) actorMessage.getRequest()
              .get(JsonKey.HEADER);
          String courseId = (String) courseMap.get(JsonKey.COURSE_ID);
          Map<String, Object> ekStepContent = getCourseObjectFromEkStep(courseId, headers);
          if (null == ekStepContent || ekStepContent.size() == 0) {
            ProjectLogger.log("Course Id not found in EkStep");
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidCourseId.getErrorCode(),
                ResponseCode.invalidCourseId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          } else {
            courseMap.put(JsonKey.COURSE_LOGO_URL, ekStepContent.get(JsonKey.APP_ICON));
            courseMap.put(JsonKey.CONTENT_ID, courseId);
            courseMap.put(JsonKey.COURSE_NAME, ekStepContent.get(JsonKey.NAME));
            courseMap.put(JsonKey.DESCRIPTION, ekStepContent.get(JsonKey.DESCRIPTION));
            courseMap.put(JsonKey.BATCH_ID, "1");
            courseMap.put(JsonKey.ADDED_BY, addedBy);
            courseMap.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
            courseMap.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
            courseMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
            courseMap.put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime()));
            courseMap.put(JsonKey.ID, generatePrimaryKey(courseMap));
            courseMap.put(JsonKey.COURSE_PROGRESS, 0);
            Response result = cassandraOperation.insertRecord(courseEnrollmentdbInfo.getKeySpace(),
                courseEnrollmentdbInfo.getTableName(), courseMap);
            sender().tell(result, self());
            return;
            }
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.invalidOperationName.getErrorCode(),
            ResponseCode.invalidOperationName.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getCourseObjectFromEkStep(String courseId,
      Map<String, String> headers) {
    if (!ProjectUtil.isStringNullOREmpty(courseId)) {
      try {
        String query = EKSTEP_COURSE_SEARCH_QUERY.replaceAll("COURSE_ID_PLACEHOLDER", courseId);
        Object[] result = EkStepRequestUtil.searchContent(query, headers);
        if (null != result && result.length > 0) {
          Object contentObject = result[0];
          Map<String, Object> map = (Map<String, Object>) contentObject;
          return map;
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
  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId);
  }

  /**
   * This method will call the background job manager and update course enroll user count.
   *
   * @param ooperation String (operation name)
   * @param courseData Object
   * @param innerOperation String
   */
  @SuppressWarnings("unused")
  private void updateCoursemanagement(String ooperation, Object courseData, String innerOperation) {
    Timeout timeout = new Timeout(
        Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
    Response userCountresponse = new Response();
    userCountresponse.put(JsonKey.OPERATION, ooperation);
    userCountresponse.put(JsonKey.COURSE_ID, courseData);
    userCountresponse.getResult().put(JsonKey.OPERATION, innerOperation);
    Patterns.ask(RequestRouterActor.backgroundJobManager, userCountresponse, timeout);
  }

}
