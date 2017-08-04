package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
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
  
  
}
