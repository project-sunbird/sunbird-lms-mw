package org.sunbird.learner.actors.bulkupload;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.Slug;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.actors.BackgroundJobManager;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import org.json.simple.parser.JSONParser;

public class BulkUploadBackGroundJobActor extends UntypedAbstractActor {

  private ActorRef backGroundActorRef;
  Util.DbInfo  bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);

  public BulkUploadBackGroundJobActor() {
    backGroundActorRef = getContext().actorOf(Props.create(BackgroundJobManager.class), "backGroundActor");
   }
  private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  private SSOManager ssoManager = new KeyCloakServiceImpl();
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("BulkUploadBackGroundJobActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.PROCESS_BULK_UPLOAD.getValue())) {
          process(actorMessage);
        }else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
    }
  }

  private void process(Request actorMessage) {
    ObjectMapper mapper = new ObjectMapper();
    String processId = (String) actorMessage.get(JsonKey.PROCESS_ID);
    Map<String,Object> dataMap = getBulkData(processId);
    int status = (int) dataMap.get(JsonKey.STATUS);
    if(!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))){
      TypeReference<List<Map<String,Object>>> mapType = new TypeReference<List<Map<String,Object>>>() {};
      List<Map<String,Object>> jsonList = null;
      try {
        jsonList = mapper.readValue((String)dataMap.get(JsonKey.DATA), mapType);
      } catch (IOException e) {
        ProjectLogger.log("Exception occurred while converting json String to List in BulkUploadBackGroundJobActor : ", e);
      }
      if(((String)dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.USER)){
        processUserInfo(jsonList,processId);
      }else if(((String)dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.ORGANISATION)){
        CopyOnWriteArrayList<Map<String,Object>> orgList = new CopyOnWriteArrayList<>(jsonList);
        processOrgInfo(orgList , dataMap);
      }else if(((String)dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.BATCH)){
        processBatchEnrollment(jsonList,processId);
      }
    }
   }

  private void processBatchEnrollment(List<Map<String, Object>> jsonList, String processId) {
  //update status from NEW to INProgress
    updateStatusForProcessing(processId);
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    List<Map<String , Object>> successResultList = new ArrayList<>();
    List<Map<String , Object>> failureResultList = new ArrayList<>();
    
    Map<String , Object> successListMap = null;
    Map<String , Object> failureListMap = null;
    for(Map<String, Object> batchMap : jsonList){
      successListMap = new HashMap<>();
      failureListMap = new HashMap<>();
      Map<String , Object> tempFailList = new HashMap<>();
      Map<String , Object> tempSuccessList = new HashMap<>();
      
      String batchId = (String) batchMap.get(JsonKey.BATCH_ID);
      Response courseBatchResult = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),
          batchId);
      String msg = validateBatchInfo(courseBatchResult);
      if(msg.equals(JsonKey.SUCCESS)){
        List<Map<String, Object>> courseList = (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
        List<String> userList = new ArrayList<>(Arrays.asList( (((String)batchMap.get(JsonKey.USER_IDs)).split(","))));
        validateBatchUserListAndAdd(courseList.get(0),batchId,userList,tempFailList,tempSuccessList);
        failureListMap.put(batchId, tempFailList.get(JsonKey.FAILURE_RESULT));
        successListMap.put(batchId, tempSuccessList.get(JsonKey.SUCCESS_RESULT));
      }else{
        batchMap.put(JsonKey.ERROR_MSG, msg);
        failureResultList.add(batchMap);
      }
      if(!successListMap.isEmpty()){
        successResultList.add(successListMap);
      }
      if(!failureListMap.isEmpty()){
        failureResultList.add(failureListMap);
      }
    }
    
    //Insert record to BulkDb table
    Map<String,Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.SUCCESS_RESULT, convertMapToJsonString(successResultList));
    map.put(JsonKey.FAILURE_RESULT, convertMapToJsonString(failureResultList));
    map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    try{
    cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    }catch(Exception e){
      ProjectLogger.log("Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ", e);
    }
  }

  private void validateBatchUserListAndAdd(Map<String, Object> courseBatchObject,String batchId, List<String> userIds,
      Map<String , Object> failList,Map<String , Object> successList) {
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    List<Map<String,Object>> failedUserList =  new ArrayList<>();
    List<Map<String,Object>> passedUserList =  new ArrayList<>();
    
    Map<String,Object> map = null;
    List<String> createdFor = (List<String>)courseBatchObject.get(JsonKey.COURSE_CREATED_FOR);
    Map<String , Boolean> participants = (Map<String , Boolean>)courseBatchObject.get(JsonKey.PARTICIPANT);
    // check whether can update user or not
    for(String userId : userIds) {
      if (!(participants.containsKey(userId))) {
        Response dbResponse = cassandraOperation
            .getRecordsByProperty(userOrgdbInfo.getKeySpace(), userOrgdbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        List<Map<String, Object>> userOrgResult = (List<Map<String, Object>>) dbResponse
            .get(JsonKey.RESPONSE);

        if (userOrgResult.isEmpty()) {
          map = new HashMap<>();
          map.put(userId , ResponseCode.userNotAssociatedToOrg.getErrorMessage());
          failedUserList.add(map);
          continue;
        }
        boolean flag = false;
        for (int i = 0; i < userOrgResult.size() && !flag; i++) {
          Map<String, Object> usrOrgDetail = userOrgResult.get(i);
          if (createdFor.contains((String) usrOrgDetail.get(JsonKey.ORGANISATION_ID))) {
            participants.put(userId, addUserCourses(batchId , (String)courseBatchObject.get(JsonKey.COURSE_ID) , null , 
                userId , (Map<String , String>)(courseBatchObject.get(JsonKey.COURSE_ADDITIONAL_INFO))));
            flag = true;
          }
        }
        if (flag) {
          map = new HashMap<>();
          map.put(userId, JsonKey.SUCCESS);
          passedUserList.add(map);
        } else {
          map = new HashMap<>();
          map.put(userId, ResponseCode.userNotAssociatedToOrg.getErrorMessage());
          failedUserList.add(map);
        }

      }else{
        map = new HashMap<>();
        map.put(userId, JsonKey.SUCCESS);
        passedUserList.add(map);
      }
    }
    courseBatchObject.put(JsonKey.PARTICIPANT , participants);
    cassandraOperation.updateRecord(dbInfo.getKeySpace() , dbInfo.getTableName() , courseBatchObject);
    successList.put(JsonKey.SUCCESS_RESULT, passedUserList);
    failList.put(JsonKey.FAILURE_RESULT, failedUserList);
    
    ProjectLogger.log("method call going to satrt for ES--.....");
    Response batchRes = new Response();
    batchRes.getResult()
        .put(JsonKey.OPERATION, ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
    batchRes.getResult().put(JsonKey.BATCH, courseBatchObject);
    ProjectLogger.log("making a call to save Course Batch data to ES");
    try {
      backGroundActorRef.tell(batchRes,self());
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occured during saving Course Batch to Es while updating Course Batch : ", ex);
    }
  }

  private Boolean addUserCourses(String batchId, String courseId, String updatedBy,
      String userId, Map<String, String> additionalCourseInfo) {

    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    Util.DbInfo coursePublishdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
    Response response = cassandraOperation.getRecordById(coursePublishdbInfo.getKeySpace() , coursePublishdbInfo.getTableName() , courseId)  ;
    List<Map<String , Object>> resultList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if(resultList.isEmpty()){
      return false;
    }
    Map<String, Object> publishStatus = resultList.get(0);

    if(Status.ACTIVE.getValue() != (Integer)publishStatus.get(JsonKey.STATUS)){
      return false;
    }

    Boolean flag = false;
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
    userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.APP_ICON));
    userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.NAME));
    userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
    if(ProjectUtil.isStringNullOREmpty(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))){
      userCourses.put(JsonKey.LEAF_NODE_COUNT, additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT));
    }
    try {
      cassandraOperation
          .insertRecord(courseEnrollmentdbInfo.getKeySpace(), courseEnrollmentdbInfo.getTableName(),
              userCourses);
      insertUserCoursesToES(userCourses);
      flag = true;
    }catch(Exception ex) {
      ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ",ex);
      flag = false;
    }
    return flag;
  }
  
  private void insertUserCoursesToES(Map<String, Object> courseMap) {
    Response response = new Response();
    response.put(JsonKey.OPERATION, ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
    response.put(JsonKey.USER_COURSES, courseMap);
    try{
      backGroundActorRef.tell(response,self());
    }catch(Exception ex){
      ProjectLogger.log("Exception Occured during saving user count to Es : ", ex);
    }
  }
  
  private String validateBatchInfo(Response courseBatchResult) {
    //check batch exist in db or not
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      return ResponseCode.invalidCourseBatchId.getErrorMessage();
    }
    Map<String, Object> courseBatchObject = courseList.get(0);
    // check whether coursebbatch type is invite only or not ...
    if(ProjectUtil.isNull(courseBatchObject.get(JsonKey.ENROLLMENT_TYPE)) || 
        !((String)courseBatchObject.get(JsonKey.ENROLLMENT_TYPE)).equalsIgnoreCase(JsonKey.INVITE_ONLY)){
      return ResponseCode.enrollmentTypeValidation.getErrorMessage();
    }
    if(ProjectUtil.isNull(courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)) || ((List)courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()){
      return ResponseCode.courseCreatedForIsNull.getErrorMessage();
    }
    return JsonKey.SUCCESS;
    
  }

  private void processOrgInfo(CopyOnWriteArrayList<Map<String, Object>> jsonList, Map<String,Object> dataMap) {

    Map<String , String> channelToRootOrgCache = new HashMap<>();
    List<Map<String , Object>> successList = new ArrayList<>();
    List<Map<String , Object>> failureList = new ArrayList<>();
    //Iteration for rootorg
    for(Map<String , Object> map : jsonList){
      try {
        if(map.containsKey(JsonKey.IS_ROOT_ORG) && isNotNull(map.get(JsonKey.IS_ROOT_ORG))) {
          Boolean isRootOrg = new Boolean((String)map.get(JsonKey.IS_ROOT_ORG));
          if(isRootOrg) {
            processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
            jsonList.remove(map);
          }
        }
      }catch(Exception ex){
        ProjectLogger.log("Exception occurs  " ,ex);
        map.put(JsonKey.ERROR_MSG , ex.getMessage());
        failureList.add(map);
      }
    }

    //Iteration for non root org
    for(Map<String , Object> map : jsonList){
      try {
        processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
      }catch(Exception ex){
        ProjectLogger.log("Exception occurs  " ,ex);
        map.put(JsonKey.ERROR_MSG , ex.getMessage());
        failureList.add(map);
      }
    }

    dataMap.put(JsonKey.SUCCESS_RESULT , convertMapToJsonString(successList));
    dataMap.put(JsonKey.FAILURE_RESULT , convertMapToJsonString(failureList));
    dataMap.put(JsonKey.STATUS , BulkProcessStatus.COMPLETED.getValue());

    cassandraOperation.updateRecord(bulkDb.getKeySpace(),bulkDb.getTableName() , dataMap);

  }

  private void processOrg(Map<String, Object> map, Map<String, Object> dataMap,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList,
      Map<String, String> channelToRootOrgCache) {

    Map<String , Object> concurrentHashMap = map;
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Object[] orgContactList=null;
    String contactDetails=null;

    if (isNull(concurrentHashMap.get(JsonKey.ORGANISATION_NAME)) || ProjectUtil.isStringNullOREmpty((String)concurrentHashMap.get(JsonKey.ORGANISATION_NAME))) {
        ProjectLogger.log("orgName is mandatory for org creation.");
        concurrentHashMap.put(JsonKey.ERROR_MSG, "orgName is mandatory for org creation.");
        failureList.add(concurrentHashMap);
        return;
    }

    if (concurrentHashMap.containsKey(JsonKey.PROVIDER) || concurrentHashMap.containsKey(JsonKey.EXTERNAL_ID)) {
      if (isNull(concurrentHashMap.get(JsonKey.PROVIDER)) || isNull(
          concurrentHashMap.get(JsonKey.EXTERNAL_ID))) {
        ProjectLogger.log("Source and external ids both should exist.");
        concurrentHashMap.put(JsonKey.ERROR_MSG , "Source and external ids both should exist.");
        failureList.add(concurrentHashMap);
        return;
      }

      if (concurrentHashMap.containsKey(JsonKey.CONTACT_DETAILS) && !ProjectUtil.isStringNullOREmpty((String)concurrentHashMap.get(JsonKey.CONTACT_DETAILS))) {

        contactDetails = (String) concurrentHashMap.get(JsonKey.CONTACT_DETAILS);
        contactDetails = contactDetails.replaceAll("'","\"");
        JSONParser parser = new JSONParser();
        try {
          JSONArray json = (JSONArray) parser.parse(contactDetails);
          ObjectMapper mapper = new ObjectMapper();
          orgContactList = mapper.readValue(contactDetails, Object[].class);

        } catch (IOException | ParseException ex) {
          ProjectLogger.log("Unable to parse Org contact Details - OrgBBulkUpload.",ex);
          concurrentHashMap.put(JsonKey.ERROR_MSG , "Unable to parse Org contact Details - OrgBBulkUpload.");
          failureList.add(concurrentHashMap);
          return;
        }
      }

      Map<String, Object> dbMap = new HashMap<>();
      dbMap.put(JsonKey.PROVIDER, concurrentHashMap.get(JsonKey.PROVIDER));
      dbMap.put(JsonKey.EXTERNAL_ID, concurrentHashMap.get(JsonKey.EXTERNAL_ID));
      Response result = cassandraOperation.getRecordsByProperties(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), dbMap);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        ProjectLogger.log("Org exist with Provider " + concurrentHashMap.get(JsonKey.PROVIDER) + " , External Id "
            + concurrentHashMap.get(JsonKey.EXTERNAL_ID));
        concurrentHashMap.put(JsonKey.ERROR_MSG , "Org exist with Provider " + concurrentHashMap.get(JsonKey.PROVIDER) + " , External Id "
            + concurrentHashMap.get(JsonKey.EXTERNAL_ID));
        failureList.add(concurrentHashMap);
        return;
      }
    }

    Boolean isRootOrg ;
    if(isNotNull(concurrentHashMap.get(JsonKey.IS_ROOT_ORG))){
      isRootOrg = new Boolean((String)concurrentHashMap.get(JsonKey.IS_ROOT_ORG));
    }else{
      isRootOrg=false;
    }
    concurrentHashMap.put(JsonKey.IS_ROOT_ORG , isRootOrg);
      if(isRootOrg){
        if(isNull(concurrentHashMap.get(JsonKey.CHANNEL))) {
          concurrentHashMap.put(JsonKey.ERROR_MSG , "Channel is mandatory for root org ");
          failureList.add(concurrentHashMap);
          return;
        }

        //check for unique root org for channel -----
          Map<String , Object> filters = new HashMap<>();
          filters.put(JsonKey.CHANNEL , (String)concurrentHashMap.get(JsonKey.CHANNEL));
          filters.put(JsonKey.IS_ROOT_ORG , true);

          Map<String , Object> esResult = elasticSearchComplexSearch(filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
          if(isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT) && isNotNull(esResult.get(JsonKey.CONTENT)) && ((List)esResult.get(JsonKey.CONTENT)).size()>0){
            concurrentHashMap.put(JsonKey.ERROR_MSG , "Root Org already exist for Channel "+concurrentHashMap.get(JsonKey.CHANNEL));
            failureList.add(concurrentHashMap);
            return;
          }
        concurrentHashMap.put(JsonKey.ROOT_ORG_ID , JsonKey.DEFAULT_ROOT_ORG_ID);
        channelToRootOrgCache.put((String)concurrentHashMap.get(JsonKey.CHANNEL) , (String)concurrentHashMap.get(JsonKey.ORGANISATION_NAME));

    }else{

        if(concurrentHashMap.containsKey(JsonKey.CHANNEL) && isNotNull(JsonKey.CHANNEL)){
          String channel = (String)concurrentHashMap.get(JsonKey.CHANNEL);
          if(channelToRootOrgCache.containsKey(channel)){
            concurrentHashMap.put(JsonKey.ROOT_ORG_ID , channelToRootOrgCache.get(channel));
          }else{
            Map<String , Object> filters = new HashMap<>();
            filters.put(JsonKey.CHANNEL , (String)concurrentHashMap.get(JsonKey.CHANNEL));
            filters.put(JsonKey.IS_ROOT_ORG , true);

            Map<String , Object> esResult = elasticSearchComplexSearch(filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
            if(isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT) && isNotNull(esResult.get(JsonKey.CONTENT)) && ((List)esResult.get(JsonKey.CONTENT)).size()>0){

              Map<String , Object> esContent = ((List<Map<String, Object>>)esResult.get(JsonKey.CONTENT)).get(0);
              concurrentHashMap.put(JsonKey.ROOT_ORG_ID , esContent.get(JsonKey.ID));
              channelToRootOrgCache.put((String)concurrentHashMap.get(JsonKey.CHANNEL) , (String)esContent.get(JsonKey.ID));

            }else{
              concurrentHashMap.put(JsonKey.ERROR_MSG , "This is not root org and No Root Org id exist for channel  "+concurrentHashMap.get(JsonKey.CHANNEL));
              failureList.add(concurrentHashMap);
              return;
            }
          }
        }else{
          concurrentHashMap.put(JsonKey.ROOT_ORG_ID , JsonKey.DEFAULT_ROOT_ORG_ID);
        }

      }

    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(1);
    concurrentHashMap.put(JsonKey.ID, uniqueId);
    concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    concurrentHashMap.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
    // allow lower case values for source and externalId to the database
    if (concurrentHashMap.get(JsonKey.PROVIDER) != null) {
      concurrentHashMap.put(JsonKey.PROVIDER, ((String) concurrentHashMap.get(JsonKey.PROVIDER)).toLowerCase());
    }
    if (concurrentHashMap.get(JsonKey.EXTERNAL_ID) != null) {
      concurrentHashMap.put(JsonKey.EXTERNAL_ID, ((String) concurrentHashMap.get(JsonKey.EXTERNAL_ID)).toLowerCase());
    }
    concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    concurrentHashMap.put(JsonKey.CREATED_BY, dataMap.get(JsonKey.UPLOADED_BY));
    concurrentHashMap.put(JsonKey.HASH_TAG_ID, uniqueId);
   //Remove the slug key if coming form user input.
    concurrentHashMap.remove(JsonKey.SLUG);
    if (concurrentHashMap.containsKey(JsonKey.CHANNEL)){
       concurrentHashMap.put(JsonKey.SLUG, Slug.makeSlug((String)concurrentHashMap.getOrDefault(JsonKey.CHANNEL, ""), true));
    }
    concurrentHashMap.put(JsonKey.CONTACT_DETAILS , contactDetails);

    try {
      Response result =
          cassandraOperation
              .insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), concurrentHashMap);
      Response orgResponse = new Response();

      // sending the org contact as List if it is null simply remove from map
      if(isNotNull(orgContactList)){
      concurrentHashMap.put(JsonKey.CONTACT_DETAILS , Arrays.asList(orgContactList));
      }

      orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
      orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
      ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
      backGroundActorRef.tell(orgResponse, self());
      successList.add(concurrentHashMap);
    }catch(Exception ex){

      ProjectLogger.log("Exception occurs  " ,ex);
      concurrentHashMap.put(JsonKey.ERROR_MSG , ex.getMessage());
      failureList.add(concurrentHashMap);
      return;
    }

  }

  private Map<String , Object> elasticSearchComplexSearch(Map<String , Object> filters , String index , String type) {

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS , filters);

    return ElasticSearchUtil.complexSearch(searchDTO , index,type);

  }


  private void processUserInfo(List<Map<String, Object>> dataMapList, String processId) {
    //update status from NEW to INProgress
    updateStatusForProcessing(processId);
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    List<Map<String, Object>> failureUserReq = new ArrayList<>();
    List<Map<String, Object>> successUserReq = new ArrayList<>();
    Map<String,Object> userMap = null;
    for(int i = 0 ; i < dataMapList.size() ; i++){
      userMap = dataMapList.get(i);
      String errMsg = validateUser(userMap);
      if(errMsg.equalsIgnoreCase(JsonKey.SUCCESS)){
        try{
          //this role is part of organization
          if ( null != userMap.get(JsonKey.ROLES)) {
            String[] userRole = ((String) userMap.get(JsonKey.ROLES)).split(",");
            List<String> list = new ArrayList<>(Arrays.asList(userRole));
            userMap.put(JsonKey.ROLES, list);
          }

          if ( null != userMap.get(JsonKey.GRADE)) {
            String[] userGrade = ((String) userMap.get(JsonKey.GRADE)).split(",");
            List<String> list = new ArrayList<>(Arrays.asList(userGrade));
            userMap.put(JsonKey.GRADE, list);
          }
          
          if ( null != userMap.get(JsonKey.SUBJECT)) {
            String[] userGrade = ((String) userMap.get(JsonKey.SUBJECT)).split(",");
            List<String> list = new ArrayList<>(Arrays.asList(userGrade));
            userMap.put(JsonKey.SUBJECT, list);
          }
          
          if ( null != userMap.get(JsonKey.LANGUAGE)) {
            String[] userGrade = ((String) userMap.get(JsonKey.LANGUAGE)).split(",");
            List<String> list = new ArrayList<>(Arrays.asList(userGrade));
            userMap.put(JsonKey.LANGUAGE, list);
          }
          userMap = insertRecordToKeyCloak(userMap);
          Map<String,Object> tempMap = new HashMap<>();
          tempMap.putAll(userMap);
          tempMap.remove(JsonKey.EMAIL_VERIFIED);
          tempMap.remove(JsonKey.PHONE_VERIFIED);
          tempMap.remove(JsonKey.POSITION);
          //Add only PUBLIC role to user
          List<String> list = new ArrayList<>();
          list.add(JsonKey.PUBLIC);
          tempMap.put(JsonKey.ROLES, list);
          //convert userName,provide,loginId,externalId.. value to lowercase
          updateMapSomeValueTOLowerCase(tempMap);
          Response response = null;
          try {
            response = cassandraOperation
                .insertRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), tempMap);
          } catch(Exception ex){
            ProjectLogger.log("Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:", ex);
            userMap.remove(JsonKey.ID);
            userMap.remove(JsonKey.PASSWORD);
            userMap.put(JsonKey.ERROR_MSG, ex.getMessage());
            failureUserReq.add(userMap);
            continue;
          } finally {
            if (null == response) {
              ssoManager.removeUser(userMap);
            }
          }
          //save successfully created user data 
          tempMap.putAll(userMap);
          tempMap.remove(JsonKey.STATUS);
          tempMap.remove(JsonKey.CREATED_DATE);
          tempMap.remove(JsonKey.CREATED_BY);
          tempMap.remove(JsonKey.ID);
          tempMap.put(JsonKey.PASSWORD,"*****");
          successUserReq.add(tempMap);
          //insert details to user_org table
          insertRecordToUserOrgTable(userMap);
          //insert details to user Ext Identity table
          insertRecordToUserExtTable(userMap);
          //update elastic search
          Response usrResponse = new Response();
          usrResponse.getResult()
              .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
          usrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
          ProjectLogger.log("making a call to save user data to ES in BulkUploadBackGroundJobActor");
          backGroundActorRef.tell(usrResponse,self());
            
        } catch(Exception ex) {
          ProjectLogger.log("Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:", ex);
          userMap.remove(JsonKey.ID);
          userMap.remove(JsonKey.PASSWORD);
          userMap.put(JsonKey.ERROR_MSG, ex.getMessage());
          failureUserReq.add(userMap);
        }
      }else{
        userMap.put(JsonKey.ERROR_MSG, errMsg);
        failureUserReq.add(userMap);
      }
     }
    //Insert record to BulkDb table
    Map<String,Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.SUCCESS_RESULT, convertMapToJsonString(successUserReq));
    map.put(JsonKey.FAILURE_RESULT, convertMapToJsonString(failureUserReq));
    map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    try{
    cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    }catch(Exception e){
      ProjectLogger.log("Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ", e);
    }
  }

  private void updateStatusForProcessing(String processId) {
  //Update status to BulkDb table
    Map<String,Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
    try{
    cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    }catch(Exception e){
      ProjectLogger.log("Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ", e);
    }
    
  }

  private String convertMapToJsonString(List<Map<String, Object>> mapList) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(mapList);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getBulkData(String processId) {
    try{
      Map<String,Object> map = new HashMap<>();
      map.put(JsonKey.ID, processId);
      map.put(JsonKey.PROCESS_START_TIME, ProjectUtil.getFormattedDate());
      map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
      cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    }catch(Exception ex){
      ProjectLogger.log("Exception occurred while updating status to bulk_upload_process "
          + "table in BulkUploadBackGroundJobActor.", ex);
    }
    Response res = cassandraOperation.getRecordById(bulkDb.getKeySpace(), bulkDb.getTableName(), processId);
    return (((List<Map<String,Object>>)res.get(JsonKey.RESPONSE)).get(0));
  }

  private void insertRecordToUserExtTable(Map<String, Object> requestMap) {
    Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
      /* update table for userName,phone,email,Aadhar No
       * for each of these parameter insert a record into db
       * for username update isVerified as true
       * and for others param this will be false
       * once verified will update this flag to true
       */

    map.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
    map.put(JsonKey.IS_VERIFIED, false);
    if (requestMap.containsKey(JsonKey.USERNAME) && !(ProjectUtil
        .isStringNullOREmpty((String) requestMap.get(JsonKey.USERNAME)))) {
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.USERNAME));
      map.put(JsonKey.EXTERNAL_ID_VALUE, JsonKey.USERNAME);
      map.put(JsonKey.IS_VERIFIED, true);

      reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.USERNAME));

      updateUserExtIdentity(map, usrExtIdDb);
    }
    if (requestMap.containsKey(JsonKey.PHONE) && !(ProjectUtil
        .isStringNullOREmpty((String) requestMap.get(JsonKey.PHONE)))) {
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      map.put(JsonKey.EXTERNAL_ID, JsonKey.PHONE);
      map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));

      if (null != (requestMap.get(JsonKey.PHONE_VERIFIED))
          &&
          (boolean) requestMap.get(JsonKey.PHONE_VERIFIED)) {
        map.put(JsonKey.IS_VERIFIED, true);
      }
      reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));

      updateUserExtIdentity(map, usrExtIdDb);
    }
    if (requestMap.containsKey(JsonKey.EMAIL) && !(ProjectUtil
        .isStringNullOREmpty((String) requestMap.get(JsonKey.EMAIL)))) {
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      map.put(JsonKey.EXTERNAL_ID, JsonKey.EMAIL);
      map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.EMAIL));

      if (null != (requestMap.get(JsonKey.EMAIL_VERIFIED)) &&
          (boolean) requestMap.get(JsonKey.EMAIL_VERIFIED)) {
        map.put(JsonKey.IS_VERIFIED, true);
      }
      reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.EMAIL));

      updateUserExtIdentity(map, usrExtIdDb);
    }
    if (requestMap.containsKey(JsonKey.AADHAAR_NO) && !(ProjectUtil
        .isStringNullOREmpty((String) requestMap.get(JsonKey.AADHAAR_NO)))) {
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      map.put(JsonKey.EXTERNAL_ID, JsonKey.AADHAAR_NO);
      map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.AADHAAR_NO));

      reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.AADHAAR_NO));

      updateUserExtIdentity(map, usrExtIdDb);
    }
  }
  
  private void updateUserExtIdentity(Map<String, Object> map, DbInfo usrExtIdDb) {
    try {
      cassandraOperation.insertRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(), map);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }

  }

  private void insertRecordToUserOrgTable(Map<String, Object> userMap) {
    Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
    reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    reqMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
    reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    reqMap.put(JsonKey.POSITION, userMap.get(JsonKey.POSITION));
    reqMap.put(JsonKey.IS_DELETED, false);
    List<String> roleList = (List<String>) userMap.get(JsonKey.ROLES);
    if(!roleList.contains(ProjectUtil.UserRole.CONTENT_CREATOR.getValue())){
      roleList.add(ProjectUtil.UserRole.CONTENT_CREATOR.getValue());
    }
    reqMap.put(JsonKey.ROLES, roleList);

    try {
      cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(), reqMap);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> insertRecordToKeyCloak(Map<String, Object> userMap) {
    
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    
    if (userMap.containsKey(JsonKey.PROVIDER) && 
        !ProjectUtil.isStringNullOREmpty((String)userMap.get(JsonKey.PROVIDER))) {
      userMap.put(JsonKey.LOGIN_ID, 
          (String)userMap.get(JsonKey.USERNAME)+"@"+(String)userMap.get(JsonKey.PROVIDER));
    } else {
      userMap.put(JsonKey.LOGIN_ID,userMap.get(JsonKey.USERNAME));
    }
   
    if (null != userMap.get(JsonKey.LOGIN_ID)) {
      String loginId = (String) userMap.get(JsonKey.LOGIN_ID);
      Response resultFrUserName = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),
          usrDbInfo.getTableName(), JsonKey.LOGIN_ID, loginId);
      if (!(((List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE)).isEmpty())) {
        throw new ProjectCommonException(
            ResponseCode.userAlreadyExist.getErrorCode(),
            ResponseCode.userAlreadyExist.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    }
    
      try {
        String userId = ssoManager.createUser(userMap);
        if (!ProjectUtil.isStringNullOREmpty(userId)) {
          userMap.put(JsonKey.USER_ID, userId);
          userMap.put(JsonKey.ID, userId);
        } else {
          throw new ProjectCommonException(
              ResponseCode.userRegUnSuccessfull.getErrorCode(),
              ResponseCode.userRegUnSuccessfull.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
        }
      } catch (Exception exception) {
        ProjectLogger.log("Exception occured while creating user in keycloak ", exception);
        throw exception;
      }
      
      userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
      
      if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.PASSWORD))) {
        userMap
            .put(JsonKey.PASSWORD, OneWayHashing.encryptVal((String) userMap.get(JsonKey.PASSWORD)));
      }
      /**
       * set role as PUBLIC by default if role is empty in request body.
       * And if roles are coming in request body, then check for PUBLIC role , if not
       * present then add PUBLIC role to the list
       *
       */

      if (userMap.containsKey(JsonKey.ROLES)) {
        List<String> roles = (List<String>) userMap.get(JsonKey.ROLES);
        if (!roles.contains(ProjectUtil.UserRole.PUBLIC.getValue())) {
          roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
          userMap.put(JsonKey.ROLES, roles);
        }
      } else {
        List<String> roles = new ArrayList<>();
        roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        userMap.put(JsonKey.ROLES, roles);
      }

      return userMap;
    }

  private String validateUser(Map<String,Object> map) {
    if (map.get(JsonKey.USERNAME) == null) {
        return ResponseCode.userNameRequired.getErrorMessage();
    }
    if (map.get(JsonKey.FIRST_NAME) == null
            || (ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.FIRST_NAME)))) {
      return ResponseCode.firstNameRequired.getErrorMessage();
    }  
    if (!(ProjectUtil.isStringNullOREmpty((String)map.get(JsonKey.EMAIL))) && !ProjectUtil.isEmailvalid((String) map.get(JsonKey.EMAIL))) {
      return ResponseCode.emailFormatError.getErrorMessage();
    }
    if(!ProjectUtil.isStringNullOREmpty((String)map.get(JsonKey.PHONE_VERIFIED))){
      try{
        map.put(JsonKey.PHONE_VERIFIED, Boolean.parseBoolean((String)map.get(JsonKey.PHONE_VERIFIED)));
      }catch(Exception ex){
        return "property phoneVerified should be instanceOf type Boolean.";
      }
    }
    if(!ProjectUtil.isStringNullOREmpty((String)map.get(JsonKey.EMAIL_VERIFIED))){
      try{
        map.put(JsonKey.EMAIL_VERIFIED, Boolean.parseBoolean((String)map.get(JsonKey.EMAIL_VERIFIED)));
      }catch(Exception ex){
        return "property emailVerified should be instanceOf type Boolean.";
      }
    }
    if(!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PROVIDER))){
      if(!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE))){
          if(null != map.get(JsonKey.PHONE_VERIFIED)){
            if(map.get(JsonKey.PHONE_VERIFIED) instanceof Boolean){
              if(!((boolean) map.get(JsonKey.PHONE_VERIFIED))){
                return ResponseCode.phoneVerifiedError.getErrorMessage();
              }
            }else{
              return "property phoneVerified should be instanceOf type Boolean.";
            }
          }else{
            return ResponseCode.phoneVerifiedError.getErrorMessage();
          }
        }
      if(null != map.get(JsonKey.EMAIL_VERIFIED)){
        if(map.get(JsonKey.EMAIL_VERIFIED) instanceof Boolean){
          if(!((boolean) map.get(JsonKey.EMAIL_VERIFIED))){
            return ResponseCode.emailVerifiedError.getErrorMessage();
          }
        }else{
          return "property emailVerified should be instanceOf type Boolean.";
        }
      }else{
        return ResponseCode.emailVerifiedError.getErrorMessage();
      } 
    }

    return JsonKey.SUCCESS;
  }

  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId);

  }
  
  /**
   * This method will make some requested key value as lower case.  
   * @param map Request
   */
  public static void updateMapSomeValueTOLowerCase(Map<String, Object> map) {
    if (map.get(JsonKey.SOURCE) != null) {
      map.put(JsonKey.SOURCE,
          ((String) map.get(JsonKey.SOURCE)).toLowerCase());
    }
    if (map.get(JsonKey.EXTERNAL_ID) != null) {
      map.put(JsonKey.EXTERNAL_ID,
          ((String) map.get(JsonKey.EXTERNAL_ID))
              .toLowerCase());
    }
    if (map.get(JsonKey.USERNAME) != null) {
      map.put(JsonKey.USERNAME,
          ((String) map.get(JsonKey.USERNAME)).toLowerCase());
    }
    if (map.get(JsonKey.USER_NAME) != null) {
      map.put(JsonKey.USER_NAME,
          ((String) map.get(JsonKey.USER_NAME)).toLowerCase());
    }
    if (map.get(JsonKey.PROVIDER) != null) {
      map.put(JsonKey.PROVIDER,
          ((String) map.get(JsonKey.PROVIDER)).toLowerCase());
    }if (map.get(JsonKey.LOGIN_ID) != null) {
      map.put(JsonKey.LOGIN_ID,
          ((String) map.get(JsonKey.LOGIN_ID)).toLowerCase());
    }

  }
  
}
