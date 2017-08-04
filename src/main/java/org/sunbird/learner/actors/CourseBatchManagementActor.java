package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle course batch related operations.
 * @author Manzarul
 */
public class CourseBatchManagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  private Util.DbInfo dbInfo = null;
  private Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);

  private ActorRef backGroundActorRef;

  public CourseBatchManagementActor() {
    backGroundActorRef = getContext().actorOf(Props.create(BackgroundJobManager.class), "backGroundActor");
   }
  
  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param message Object  is an instance of Request
   */
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("Batch Management -onReceive called");
        dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
        Request actorMessage = (Request) message;
        String requestedOperation = actorMessage.getOperation();
        if (requestedOperation.equalsIgnoreCase(ActorOperations.CREATE_BATCH.getValue())) {
          createCourseBatch(actorMessage);
        }else if (requestedOperation.equalsIgnoreCase(ActorOperations.ADD_USER_TO_BATCH.getValue())) {
          addUserCourseBatch(actorMessage);
        }else if (requestedOperation.equalsIgnoreCase(ActorOperations.GET_COURSE_BATCH_DETAIL.getValue())) {
          getCourseBatchDetail(actorMessage);
        }  else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
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
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void getCourseBatchDetail(Request actorMessage) {

    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    String batchId = (String)req.get(JsonKey.BATCH_ID);
    Response result = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
        batchId);
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    sender().tell(result , self());
  }

  private void addUserCourseBatch(Request actorMessage) {

    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    Response response = new Response();

    String batchId = (String)req.get(JsonKey.BATCH_ID);
    //check bbatch exist in db or not
    Response result = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
        batchId);
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> courseObject = courseList.get(0);
    // check whether coursebbatch type is invite only or not ...
    if(ProjectUtil.isNull(courseObject.get(JsonKey.ENROLLMENT_TYPE)) || !((String)courseObject.get(JsonKey.ENROLLMENT_TYPE)).equalsIgnoreCase(JsonKey.INVITE_ONLY)){
     //TODO : provide proper error
      throw new ProjectCommonException(
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if(ProjectUtil.isNull(courseObject.get(JsonKey.COURSE_CREATED_FOR)) || ((List)courseObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()){
      //TODO : provide proper error
      throw new ProjectCommonException(
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    List<String> createdFor = (List<String>)courseObject.get(JsonKey.COURSE_CREATED_FOR);
    List<String> participants = (List<String>)courseObject.get(JsonKey.PARTICIPANTS);
    // check whether can update user or not
    List<String> userIds = (List<String>)req.get(JsonKey.USER_IDs);

    for(String userId : userIds){
      Response dbResponse = cassandraOperation.getRecordsByProperty(userOrgdbInfo.getKeySpace() , userOrgdbInfo.getTableName() , JsonKey.USER_ID , userId);
      List<Map<String , Object>> userOrgResult = (List<Map<String , Object>>)dbResponse.get(JsonKey.RESPONSE);

      if(userOrgResult.isEmpty()){
        //TODO: provide the proper error like user id does not exist
        throw new ProjectCommonException(
            ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
            ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }

      boolean flag = false;
      for(int i=0;i<userOrgResult.size()&&!flag;i++){
        Map<String ,  Object> usrOrgDetail = userOrgResult.get(i);
        if(createdFor.contains((String)usrOrgDetail.get(JsonKey.ORGANISATION_ID))){
          if(!(participants.contains(userId))) {
            participants.add(userId);
          }
          flag = true;
        }
      }
      if(flag){
        response.getResult().put(userId , JsonKey.SUCCESS);
      }else{
        response.getResult().put(userId , JsonKey.FAILED);
      }

    }
    courseObject.put(JsonKey.PARTICIPANTS , participants);
    cassandraOperation.updateRecord(dbInfo.getKeySpace() , dbInfo.getTableName() , courseObject);
    sender().tell(response , self());
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
        ProjectCommonException projectCommonException = new ProjectCommonException(
            ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
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

        result = cassandraOperation
            .updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
        sender().tell(result, self());

      }
    }

  }

  /**
   * This method will create course under cassandra db.
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void createCourseBatch(Request actorMessage) {
    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    // Do the check for course id null or empty.
    if (req.get(JsonKey.COURSE_ID) == null || ProjectUtil
        .isStringNullOREmpty((String) req.get(JsonKey.COURSE_ID))) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getRequest().get(JsonKey.HEADER);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headers);
    if (null == ekStepContent || ekStepContent.size() == 0) {
      ProjectLogger.log("Course Id not found in EkStep");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String enrolmentReq = (String) req.get(JsonKey.ENROLMENTTYPE);
    List<String> createdFor = new ArrayList<>();
    if (req.containsKey(JsonKey.COURSE_CREATED_FOR)
        && req.get(JsonKey.COURSE_CREATED_FOR) instanceof List) {
      createdFor = (List) req.get(JsonKey.COURSE_CREATED_FOR);
    }
    if (ProjectUtil.EnrolmentType.inviteOnly.getVal()
        .equalsIgnoreCase(enrolmentReq)) {
      if (createdFor != null) {
        for (String orgId : createdFor) {
          if (!isOrgValid(orgId)) {
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidOrgId.getErrorCode(),
                ResponseCode.invalidOrgId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
        }
      }
    }
    String courseCreator = (String) ekStepContent.get(JsonKey.CREATED_BY);
    String createdBy =
        (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    String uniqueId =
        ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.COURSE_ID, courseId);
    req.put(JsonKey.COURSE_CREATOR, courseCreator);
    req.put(JsonKey.CREATED_BY, createdBy);
    req.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    Response result = cassandraOperation.insertRecord(dbInfo.getKeySpace(),
        dbInfo.getTableName(), req);
    List<Map<String, Object>> responseList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.BATCH_ID, uniqueId);
    responseList.add(map);
    result.put(Constants.RESPONSE, responseList);
    sender().tell(result, self());
  }

  /**
   * This method will provide user name based on user id if user not found
   * then it will return null.
   *
   * @param userId String
   * @return String
   */
  @SuppressWarnings("unchecked")
  private String getUserNamebyUserId(String userId) {

    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response result = cassandraOperation
        .getRecordById(userdbInfo.getKeySpace(), userdbInfo.getTableName(), userId);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return (String) (list.get(0).get(JsonKey.USERNAME));
    }
    return null;
  }
 
  /**
   * THis method will do the organization validation.
   * @param orgId String
   * @return boolean
   */
  private boolean isOrgValid(String orgId) {
    Map<String, Object> resp = ElasticSearchUtil.getDataByIdentifier(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.organisation.getTypeName(), orgId);
    if (resp != null && resp.size() > 0) {
      ProjectLogger.log("organisation found in ES with id ==" + orgId);
      return true;
    }
    ProjectLogger.log("organisation not found in ES with id ==" + orgId);
    return false;
  }
  
  
}
