package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle course batch related operations.
 * @author Manzarul
 */
public class CourseBatchManagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  private Util.DbInfo dbInfo = null;
  private Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
  private Util.DbInfo coursePublishdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
  private static String EKSTEP_COURSE_SEARCH_QUERY = "{\"request\": {\"filters\":{\"contentType\": [\"Course\"], \"objectType\": [\"Content\"], \"identifier\": \"COURSE_ID_PLACEHOLDER\", \"status\": \"Live\"},\"limit\": 1}}";

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
        }if (requestedOperation.equalsIgnoreCase(ActorOperations.UPDATE_BATCH.getValue())) {
          updateCourseBatch(actorMessage);
        } if (requestedOperation.equalsIgnoreCase(ActorOperations.GET_BATCH.getValue())) {
          getCourseBatch(actorMessage);
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

  private void getCourseBatch(Request actorMessage) {

    Map<String, Object> map = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    Map<String, Object> result = ElasticSearchUtil
        .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(), (String) map.get(JsonKey.BATCH_ID));
    Response response = new Response();
    if (null != result) {
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());

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
    Response courseBatchResult = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
        batchId);
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> courseBatchObject = courseList.get(0);
    // check whether coursebbatch type is invite only or not ...
    if(ProjectUtil.isNull(courseBatchObject.get(JsonKey.ENROLLMENT_TYPE)) || !((String)courseBatchObject.get(JsonKey.ENROLLMENT_TYPE)).equalsIgnoreCase(JsonKey.INVITE_ONLY)){
     //TODO : provide proper error
      throw new ProjectCommonException(
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if(ProjectUtil.isNull(courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)) || ((List)courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()){
      //TODO : provide proper error
      throw new ProjectCommonException(
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
          ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    List<String> createdFor = (List<String>)courseBatchObject.get(JsonKey.COURSE_CREATED_FOR);
    Map<String , Boolean> participants = (Map<String , Boolean>)courseBatchObject.get(JsonKey.PARTICIPANT);
    // check whether can update user or not
    List<String> userIds = (List<String>)req.get(JsonKey.USER_IDs);

    for(String userId : userIds) {
      if (!(participants.containsKey(userId))) {
        Response dbResponse = cassandraOperation
            .getRecordsByProperty(userOrgdbInfo.getKeySpace(), userOrgdbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        List<Map<String, Object>> userOrgResult = (List<Map<String, Object>>) dbResponse
            .get(JsonKey.RESPONSE);

        if (userOrgResult.isEmpty()) {
          //TODO: provide the proper error like user id does not exist
          throw new ProjectCommonException(
              ResponseCode.publishedCourseCanNotBeUpdated.getErrorCode(),
              ResponseCode.publishedCourseCanNotBeUpdated.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        boolean flag = false;
        for (int i = 0; i < userOrgResult.size() && !flag; i++) {
          Map<String, Object> usrOrgDetail = userOrgResult.get(i);
          if (createdFor.contains((String) usrOrgDetail.get(JsonKey.ORGANISATION_ID))) {
            participants.put(userId, addUserCourses(batchId , (String)courseBatchObject.get(JsonKey.COURSE_ID) , updatedBy , userId));
            flag = true;
          }
        }
        if (flag) {
          response.getResult().put(userId, JsonKey.SUCCESS);
        } else {
          response.getResult().put(userId, JsonKey.FAILED);
        }

      }
    }

    courseBatchObject.put(JsonKey.PARTICIPANT , participants);
    cassandraOperation.updateRecord(dbInfo.getKeySpace() , dbInfo.getTableName() , courseBatchObject);
    sender().tell(response , self());
  }

  private Boolean addUserCourses(String batchId, String courseId, String updatedBy,
      String userId) {

    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);

    Map<String , Object> userCourses = new HashMap<>();
    userCourses.put(JsonKey.USER_ID , userId);
    userCourses.put(JsonKey.BATCH_ID , batchId);
    userCourses.put(JsonKey.COURSE_ID , courseId);
    userCourses.put(JsonKey.ID , generatePrimaryKey(userCourses));
    userCourses.put(JsonKey.CONTENT_ID, courseId);
    userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
    userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    userCourses.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    userCourses.put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime()));
    userCourses.put(JsonKey.COURSE_PROGRESS, 0);

    Boolean flag = false;


    return flag;



  }

  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId);

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
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getRequest().get(JsonKey.HEADER);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headers);
    if (null == ekStepContent || ekStepContent.size() == 0) {
      ProjectLogger.log("Course Id not found in EkStep");
      throw new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    String enrolmentType = (String) req.get(JsonKey.ENROLMENTTYPE);
    List<String> createdFor = new ArrayList<>();
    if (req.containsKey(JsonKey.COURSE_CREATED_FOR)
        && req.get(JsonKey.COURSE_CREATED_FOR) instanceof List) {
      createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
    }
    if (ProjectUtil.EnrolmentType.inviteOnly.getVal()
        .equalsIgnoreCase(enrolmentType)) {
      if (createdFor != null) {
        for (String orgId : createdFor) {
          if (!isOrgValid(orgId)) {
            throw new ProjectCommonException(
                ResponseCode.invalidOrgId.getErrorCode(),
                ResponseCode.invalidOrgId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }
      }
    }
    if(req.containsKey(JsonKey.MENTORS)
        && req.get(JsonKey.MENTORS) instanceof List){
      List<String> mentors = (List<String>) req.get(JsonKey.MENTORS);
      for(String userId : mentors){
        Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), userId);
        //check whether is_deletd true or false
        if((ProjectUtil.isNull(result))||(ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED) &&
            ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))&&
            (Boolean)result.get(JsonKey.IS_DELETED))){
          throw new ProjectCommonException(
              ResponseCode.invalidUserId.getErrorCode(),
              ResponseCode.invalidUserId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
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
    result.put(JsonKey.BATCH_ID, uniqueId);
    sender().tell(result, self());

    if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("method call going to satrt for ES--.....");
      Response response = new Response();
      response.getResult()
          .put(JsonKey.OPERATION, ActorOperations.INSERT_COURSE_BATCH_ES.getValue());
      response.getResult().put(JsonKey.BATCH, req);
      ProjectLogger.log("making a call to save Course Batch data to ES");
      try {
        backGroundActorRef.tell(response,self());
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occured during saving Course Batch to Es while creating Course Batch : ", ex);
      }
    } else {
      ProjectLogger.log("no call for ES to save Course Batch");
    }
  }

  /**
   * This method will allow user to update the course batch details.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void updateCourseBatch(Request actorMessage) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace(),
        dbInfo.getTableName(), (String)req.get(JsonKey.ID));
    List<Map<String,Object>> resList = ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE));
    if(null != resList && ! resList.isEmpty()){
      Map<String, Object> res = resList.get(0);
      if(req.containsKey(JsonKey.START_DATE)){
        Date dbBatchStartDate = null;
        Date reqStartdate = null;
        try {
          dbBatchStartDate = format.parse((String)res.get(JsonKey.START_DATE));
          reqStartdate = format.parse((String)req.get(JsonKey.START_DATE));
        } catch (ParseException e) {
          ProjectLogger.log("Exception occured while parsing date in CourseBatchManagementActor ", e);
        }
        if (dbBatchStartDate.before(reqStartdate)) {
          throw new ProjectCommonException(
              ResponseCode.courseBatchStartDateError.getErrorCode(),
              ResponseCode.courseBatchStartDateError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }

      String enrolmentType = "";
      if(req.containsKey(JsonKey.ENROLMENTTYPE)){
        enrolmentType = (String) req.get(JsonKey.ENROLMENTTYPE);
      }else{
        enrolmentType = (String) res.get(JsonKey.ENROLMENTTYPE);
      }
      List<String> createdFor = new ArrayList<>();
      if (req.containsKey(JsonKey.COURSE_CREATED_FOR)
          && req.get(JsonKey.COURSE_CREATED_FOR) instanceof List) {
        createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
      }

      if(null != createdFor){
        List<String> dbValueCreatedFor = (List<String>) res.get(JsonKey.COURSE_CREATED_FOR);
        if (ProjectUtil.EnrolmentType.inviteOnly.getVal()
            .equalsIgnoreCase(enrolmentType)) {
          if (createdFor != null) {
            for (String orgId : createdFor) {
              if(!dbValueCreatedFor.contains(orgId)){
                if (!isOrgValid(orgId)) {
                  throw new ProjectCommonException(
                      ResponseCode.invalidOrgId.getErrorCode(),
                      ResponseCode.invalidOrgId.getErrorMessage(),
                      ResponseCode.CLIENT_ERROR.getResponseCode());
                }
              }
            }
          }
        }else{
          //enrollmentType is open
          if(null != createdFor)
          for(String orgId : createdFor){
            if(!dbValueCreatedFor.contains(orgId)){
              dbValueCreatedFor.add(orgId);
            }
          }
        }
        req.put(JsonKey.COURSE_CREATED_FOR, dbValueCreatedFor);
      }

      if(req.containsKey(JsonKey.MENTORS)
          && req.get(JsonKey.MENTORS) instanceof List){
        List<String> mentors = (List<String>) req.get(JsonKey.MENTORS);
        List<String> dnMentorsValue = (List<String>) res.get(JsonKey.MENTORS);
        if(null != mentors){
          for(String userId : mentors){
            if(!dnMentorsValue.contains(userId)){
            Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(), userId);
            //check whether is_deletd true or false
            if((ProjectUtil.isNull(result))||(ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED) &&
                ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))&&
                (Boolean)result.get(JsonKey.IS_DELETED))){
              throw new ProjectCommonException(
                  ResponseCode.invalidUserId.getErrorCode(),
                  ResponseCode.invalidUserId.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
              }
            dnMentorsValue.add(userId);
            }
          }
        }
        req.put(JsonKey.MENTORS, dnMentorsValue);
      }

      Response result = cassandraOperation.updateRecord(dbInfo.getKeySpace(),
          dbInfo.getTableName(), req);
      sender().tell(result, self());

      if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
        ProjectLogger.log("method call going to satrt for ES--.....");
        Response batchRes = new Response();
        batchRes.getResult()
            .put(JsonKey.OPERATION, ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
        batchRes.getResult().put(JsonKey.BATCH, req);
        ProjectLogger.log("making a call to save Course Batch data to ES");
        try {
          backGroundActorRef.tell(batchRes,self());
        } catch (Exception ex) {
          ProjectLogger.log("Exception Occured during saving Course Batch to Es while updating Course Batch : ", ex);
        }
      } else {
        ProjectLogger.log("no call for ES to save Course Batch");
      }
    }

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

  public static Map<String, Object> getCourseObjectFromEkStep(String courseId,
      Map<String, String> headers) {
    if (!ProjectUtil.isStringNullOREmpty(courseId)) {
      try {
        String query = EKSTEP_COURSE_SEARCH_QUERY.replaceAll("COURSE_ID_PLACEHOLDER", courseId);
        Object[] result = EkStepRequestUtil.searchContent(query, headers);
        if (null != result && result.length > 0) {
          Object contentObject = result[0];
          return (Map<String, Object>) contentObject;
        }
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
    return null;
  }

  
}
