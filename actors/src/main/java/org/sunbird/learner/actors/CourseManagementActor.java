package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle course management operation on organization level.
 *
 * @author Manzarul
 */
public class CourseManagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo dbInfo = null;
  private String coursePublishedBody =
      "{\"request\":{\"content\":{\"lastPublishedBy\": \"userId\"}}}";

  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param message Object is an instance of Request
   */
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("CourseManagementActor-onReceive called");
        dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
        Request actorMessage = (Request) message;
        String requestedOperation = actorMessage.getOperation();
        if (requestedOperation.equalsIgnoreCase(ActorOperations.CREATE_COURSE.getValue())) {
          createCourse(actorMessage);
        } else if (requestedOperation.equalsIgnoreCase(ActorOperations.UPDATE_COURSE.getValue())) {
          updateCourse(actorMessage);
        } else if (requestedOperation.equalsIgnoreCase(ActorOperations.PUBLISH_COURSE.getValue())) {
          publishCourse(actorMessage);
        } else if (requestedOperation.equalsIgnoreCase(ActorOperations.DELETE_COURSE.getValue())) {
          deleteCourse(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  /**
   * This method will delete the course from cassandra.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void deleteCourse(Request actorMessage) {

    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.COURSE);
    String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    Map<String, Object> queryMap = new LinkedHashMap<String, Object>();
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      queryMap.put(JsonKey.UPDATED_BY_NAME, updatedByName);
    }

    queryMap.put(JsonKey.ID, (String) req.get(JsonKey.COURSE_ID));
    queryMap.put(JsonKey.STATUS, ProjectUtil.CourseMgmtStatus.RETIRED.getValue());
    queryMap.put(JsonKey.UPDATED_BY, updatedBy);
    Response result =
        cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
    sender().tell(result, self());
  }

  /**
   * This method will do the course published. in cassandra db course status becomes live and it
   * will collect course related data from EkStep and update some field under course management
   * table and store the data inside sunbird ES.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void publishCourse(Request actorMessage) {
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.COURSE);
    req.put(JsonKey.ID, (String) req.get(JsonKey.COURSE_ID));
    String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getRequest().get(JsonKey.HEADER);
    String resposne = null;
    try {

      String ekStepBaseUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
      if (ProjectUtil.isStringNullOREmpty(ekStepBaseUrl)) {
        ekStepBaseUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL);
      }

      resposne = HttpUtil.sendPostRequest(
          ekStepBaseUrl
              + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_COURSE_PUBLISH_URL) + "/"
              + (String) req.get(JsonKey.COURSE_ID),
          coursePublishedBody.replace("userId", updatedBy), headers);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      sender().tell(e, self());
      return;
    }
    ProjectLogger.log("Resposne for Course published==" + resposne);
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID, (String) req.get(JsonKey.COURSE_ID));
    map.put(JsonKey.STATUS, ProjectUtil.CourseMgmtStatus.DRAFT.ordinal());
    map.put(JsonKey.SUBMIT_DATE, ProjectUtil.getFormattedDate());
    Response result =
        cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), map);
    sender().tell(result, self());
  }

  /**
   * This method will allow user to update the course details.Only Draft course details can be
   * updated. once course is live then updated is not allowed.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void updateCourse(Request actorMessage) {

    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.COURSE);
    String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

    String updatedByName = null;
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      updatedByName = getUserNamebyUserId(updatedBy);
    }

    Response result = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
        (String) req.get(JsonKey.COURSE_ID));
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(courseList.isEmpty())) {
      Map<String, Object> courseObject = courseList.get(0);
      if (((String) courseObject.get(JsonKey.STATUS))
          .equalsIgnoreCase(ProjectUtil.CourseMgmtStatus.LIVE.getValue())) {
        ProjectCommonException projectCommonException =
            new ProjectCommonException(ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
                ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(projectCommonException, self());
      } else {

        Map<String, Object> queryMap = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : req.entrySet()) {
          queryMap.put(entry.getKey(), entry.getValue());
        }
        queryMap.put(JsonKey.ID, (String) req.get(JsonKey.COURSE_ID));
        queryMap.put(JsonKey.UPDATED_BY, updatedBy);
        queryMap.put(JsonKey.UPDATED_BY_NAME, updatedByName);
        queryMap.remove(JsonKey.COURSE_ID);

        result =
            cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
        sender().tell(result, self());

      }
    }

  }

  /**
   * This method will create course under cassandra db.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void createCourse(Request actorMessage) {

    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.COURSE);
    String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      req.put(JsonKey.ADDED_BY_NAME, updatedByName);
      req.put(JsonKey.ADDED_BY, updatedBy);
    }

    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.COURSE_ID, uniqueId);
    req.put(JsonKey.STATUS, ProjectUtil.CourseMgmtStatus.DRAFT.getValue());
    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    Response result =
        cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), req);
    List<Map<String, Object>> responseList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.COURSE_ID, uniqueId);
    responseList.add(map);
    result.put(Constants.RESPONSE, responseList);
    sender().tell(result, self());
  }

  /**
   * This method will provide user name based on user id if user not found then it will return null.
   *
   * @param userId String
   * @return String
   */
  @SuppressWarnings("unchecked")
  private String getUserNamebyUserId(String userId) {

    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(),
        userdbInfo.getTableName(), userId);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return (String) (list.get(0).get(JsonKey.USERNAME));
    }
    return null;
  }

}
