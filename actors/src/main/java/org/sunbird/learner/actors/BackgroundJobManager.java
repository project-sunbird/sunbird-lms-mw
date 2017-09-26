package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DataMaskingService;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.models.util.datasecurity.impl.DefaultEncryptionServivceImpl;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/**
 * This class will handle all the background job.
 * Example when ever course is published then this job will
 * collect course related data from EKStep and update with Sunbird.
 *
 * @author Manzarul
 * @author Amit Kumar
 */
public class BackgroundJobManager extends UntypedAbstractActor {

  private static Map<String, String> headerMap = new HashMap<>();
  private static Util.DbInfo dbInfo = null;

  static {
    headerMap.put("content-type", "application/json");
    headerMap.put("accept", "application/json");
  }

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Response) {
      ProjectLogger.log("BackgroundJobManager  onReceive called");
      if (dbInfo == null) {
        dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_MANAGEMENT_DB);
      }

      Response actorMessage = (Response) message;
      String requestedOperation = (String) actorMessage.get(JsonKey.OPERATION);
      ProjectLogger.log("Operation name is ==" + requestedOperation);
      if (requestedOperation.equalsIgnoreCase(ActorOperations.PUBLISH_COURSE.getValue())) {
        manageBackgroundJob(actorMessage.getResult());

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_COUNT.getValue())) {
        updateUserCount(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue())) {
        updateUserInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue())) {
        updateOrgInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue())) {
        insertOrgInfoToEs(actorMessage);

      }else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.INSERT_COURSE_BATCH_ES.getValue())) {
        insertCourseBatchInfoToEs(actorMessage);

      }else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue())) {
        updateCourseBatchInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_ORG_ES.getValue())) {
        updateUserOrgInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.REMOVE_USER_ORG_ES.getValue())) {
        removeUserOrgInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_ROLES_ES.getValue())) {
        updateUserRoleToEs(actorMessage);
      }else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USR_COURSES_INFO_ELASTIC.getValue())) {
        updateUserCourseInfoToEs(actorMessage);

      }else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue())) {
        insertUserCourseInfoToEs(actorMessage);

      }else if (requestedOperation.equalsIgnoreCase(ActorOperations.ADD_USER_BADGE_BKG.getValue())){
         addBadgeToUserprofile (actorMessage);
      } else if (requestedOperation
              .equalsIgnoreCase(ActorOperations.INSERT_USER_NOTES_ES.getValue())) {
        insertUserNotesToEs(actorMessage);
      } else if (requestedOperation
            .equalsIgnoreCase(ActorOperations.UPDATE_USER_NOTES_ES.getValue())) {
        updateUserNotesToEs(actorMessage);
      } else {
        ProjectLogger.log("UNSUPPORTED OPERATION");
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.invalidOperationName.getErrorCode(),
            ResponseCode.invalidOperationName.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        ProjectLogger.log("UnSupported operation in Background Job Manager", exception);
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE FOR BACKGROUND JOB MANAGER");
    }
  }

  
  /**
   * @param actorMessage
   */
  private void addBadgeToUserprofile(Response actorMessage) {
     Map<String,Object> userBadgeMap = actorMessage.getResult();
     userBadgeMap.remove(JsonKey.OPERATION);
     DbInfo userbadge = Util.dbInfoMap.get(JsonKey.USER_BADGES_DB);
     Response response = cassandraOperation.getRecordsByProperties(userbadge.getKeySpace(), userbadge.getTableName(), userBadgeMap);
     if(response != null && response.get(JsonKey.RESPONSE) != null) {
       List<Map<String,Object>> badgesList = (List<Map<String,Object>>)response.get(JsonKey.RESPONSE);
       if (badgesList != null && badgesList.size()>0) {
         badgesList= removeDataFromMap(badgesList);
         Map<String,Object> map = new HashMap<>();
         map.put(JsonKey.BADGES, badgesList);
       boolean updateResponse = updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
           ProjectUtil.EsType.user.getTypeName(),
           (String)userBadgeMap.get(JsonKey.RECEIVER_ID), map);
       ProjectLogger.log("User badge update response==" + updateResponse);
       }
     }else {
       ProjectLogger.log("No data found user badges to sync with user===" , LoggerEnum.INFO.name());
     }
  }

  
  
  public static List<Map<String, Object>> removeDataFromMap(
      List<Map<String, Object>> listOfMap) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Map<String, Object> map : listOfMap) {
      Map<String, Object> innermap = new HashMap<>();
      innermap.put(JsonKey.ID, map.get(JsonKey.ID));
      innermap.put(JsonKey.BADGE_TYPE_ID, map.get(JsonKey.BADGE_TYPE_ID));
      innermap.put(JsonKey.RECEIVER_ID, map.get(JsonKey.RECEIVER_ID));
      innermap.put(JsonKey.CREATED_DATE, map.get(JsonKey.CREATED_DATE));
      innermap.put(JsonKey.CREATED_BY, map.get(JsonKey.CREATED_BY));
      list.add(innermap);
    }
    return list;
  }

  private void updateUserRoleToEs(Response actorMessage) {
    List<String> roles = (List<String>) actorMessage.get(JsonKey.ROLES);
    String type = (String) actorMessage.get(JsonKey.TYPE);
    String orgId = (String) actorMessage.get(JsonKey.ORGANISATION_ID);
    Map<String, Object> result = ElasticSearchUtil
        .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), (String) actorMessage.get(JsonKey.USER_ID));
    if(type.equals(JsonKey.USER)){
      result.put(JsonKey.ROLES, roles);
    }else if(type.equals(JsonKey.ORGANISATION)){
      List<Map<String,Object>> roleMapList = (List<Map<String, Object>>) result.get(JsonKey.ORGANISATIONS);
      if(null != roleMapList){
          for(Map<String,Object> map : roleMapList){
            if((((String)map.get(JsonKey.USER_ID)).equalsIgnoreCase((String) actorMessage.get(JsonKey.USER_ID))) && 
                (((String)map.get(JsonKey.ORGANISATION_ID)).equalsIgnoreCase(orgId))){
              map.put(JsonKey.ROLES, roles);
            }
          }
      }
    }
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        (String) result.get(JsonKey.IDENTIFIER), result);
  }
  
  private void updateUserCourseInfoToEs(Response actorMessage) {

    Map<String,Object> batch = (Map<String, Object>) actorMessage.get(JsonKey.USER_COURSES);
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.usercourses.getTypeName(),
        (String)batch.get(JsonKey.ID), batch);
  }

  private void insertUserCourseInfoToEs(Response actorMessage) {

    Map<String,Object> batch = (Map<String, Object>) actorMessage.get(JsonKey.USER_COURSES);
    insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.usercourses.getTypeName(),
        (String) batch.get(JsonKey.ID), batch);

  }

  private void removeUserOrgInfoToEs(Response actorMessage) {
    Map<String, Object> orgMap = (Map<String, Object>) actorMessage.get(JsonKey.USER);
    Map<String, Object> result = ElasticSearchUtil
        .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), (String) orgMap.get(JsonKey.USER_ID));
    if(result.containsKey(JsonKey.ORGANISATIONS) && null != result.get(JsonKey.ORGANISATIONS)){
      List<Map<String,Object>> orgMapList = (List<Map<String, Object>>) result.get(JsonKey.ORGANISATIONS);
      if(null != orgMapList){
         Iterator<Map<String, Object>> itr = orgMapList.iterator();
         while(itr.hasNext()){
           Map<String,Object> map = (Map<String, Object>) itr.next();
          if((((String)map.get(JsonKey.USER_ID)).equalsIgnoreCase((String)orgMap.get(JsonKey.USER_ID))) &&
              (((String)map.get(JsonKey.ORGANISATION_ID)).equalsIgnoreCase((String)orgMap.get(JsonKey.ORGANISATION_ID)))){
            itr.remove();
          }
        }
      }
      }
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        (String) result.get(JsonKey.IDENTIFIER), result);

  }

  private void updateUserOrgInfoToEs(Response actorMessage) {
    Map<String, Object> orgMap = (Map<String, Object>) actorMessage.get(JsonKey.USER);
    Map<String, Object> result = ElasticSearchUtil
        .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), (String) orgMap.get(JsonKey.USER_ID));
    if(result.containsKey(JsonKey.ORGANISATIONS) && null != result.get(JsonKey.ORGANISATIONS)){
      List<Map<String,Object>> orgMapList = (List<Map<String, Object>>) result.get(JsonKey.ORGANISATIONS);
        orgMapList.add(orgMap);
      }else{
        List<Map<String,Object>> mapList = new ArrayList<>();
        mapList.add(orgMap);
        result.put(JsonKey.ORGANISATIONS, mapList);
      }
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        (String) result.get(JsonKey.IDENTIFIER), result);
  }

  private void updateCourseBatchInfoToEs(Response actorMessage) {
    Map<String,Object> batch = (Map<String, Object>) actorMessage.get(JsonKey.BATCH);
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName(),
        (String)batch.get(JsonKey.ID), batch);
  }

  private void insertCourseBatchInfoToEs(Response actorMessage) {
    Map<String, Object> batch =
        (Map<String, Object>) actorMessage.get(JsonKey.BATCH);
    Map<String,String> map = (Map<String, String>) batch.get(JsonKey.COURSE_ADDITIONAL_INFO);
    String status = map.get(JsonKey.STATUS);
    if(ProjectUtil.CourseMgmtStatus.LIVE.getValue().equalsIgnoreCase(status)){
      ProjectLogger.log("Course is live.");
     // enrollParticipants(batch);
    }
    // making call to register tag
    registertag(
        (String) batch.getOrDefault(JsonKey.HASH_TAG_ID,
            (String) batch.get(JsonKey.ID)),
        "{}", CourseBatchSchedulerUtil.headerMap);
    //register tag for course 
    registertag(
        (String) batch.getOrDefault(JsonKey.COURSE_ID,
            (String) batch.get(JsonKey.COURSE_ID)),
        "{}", CourseBatchSchedulerUtil.headerMap);
    
   /* insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName(), (String) batch.get(JsonKey.ID),
        batch);*/
  }

  private void enrollParticipants(Map<String, Object> batch) {
    Util.DbInfo courseBatchDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    Map<String,String> additionalCourseInfo = (Map<String, String>) batch.get(JsonKey.COURSE_ADDITIONAL_INFO);
    Map<String,Boolean> participants = (Map<String, Boolean>) batch.get(JsonKey.PARTICIPANT);
    for(Map.Entry<String,Boolean> entry : participants.entrySet()){
      if(!entry.getValue()){
    	Timestamp ts = new Timestamp(new Date().getTime());
        Map<String , Object> userCourses = new HashMap<>();
        userCourses.put(JsonKey.USER_ID , entry.getKey());
        userCourses.put(JsonKey.BATCH_ID , batch.get(JsonKey.ID));
        userCourses.put(JsonKey.COURSE_ID , batch.get(JsonKey.COURSE_ID));
        userCourses.put(JsonKey.ID , generatePrimaryKey(userCourses));
        userCourses.put(JsonKey.CONTENT_ID, batch.get(JsonKey.COURSE_ID));
        userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
        userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
        userCourses.put(JsonKey.STATUS, (int)batch.get(JsonKey.STATUS));
        userCourses.put(JsonKey.DATE_TIME, ts);
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
          // TODO: for some reason, ES indexing is failing with Timestamp value. need to check and correct it.
          userCourses.put(JsonKey.DATE_TIME, ProjectUtil.formatDate(ts));
          insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.usercourses.getTypeName(),
              (String) batch.get(JsonKey.ID), userCourses);
          //update participant map value as true
          entry.setValue(true);
        }catch(Exception ex) {
          ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ",ex);
        }
      }
    }
    ProjectLogger.log("Adding participants to user course table completed");
    Map<String,Object> updatedBatch = new HashMap<>();
    updatedBatch.put(JsonKey.ID, batch.get(JsonKey.ID));
    updatedBatch.put(JsonKey.PARTICIPANT, participants);
    ProjectLogger.log("Updating participants to batch course table started");
    cassandraOperation.updateRecord(courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), updatedBatch);
    ProjectLogger.log("Updating participants to batch course table completed");
  
    
  }
  
  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId);

  }

  @SuppressWarnings("unchecked")
  private void insertOrgInfoToEs(Response actorMessage) {
    ProjectLogger.log("Calling method to save inside Es==");
    Map<String, Object> orgMap =
        (Map<String, Object>) actorMessage.get(JsonKey.ORGANISATION);
    if (ProjectUtil.isNotNull(orgMap)) {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      String id = (String) orgMap.get(JsonKey.ID);
      Response orgResponse = cassandraOperation
          .getRecordById(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), id);
      List<Map<String, Object>> orgList =
          (List<Map<String, Object>>) orgResponse.getResult()
              .get(JsonKey.RESPONSE);
      Map<String, Object> esMap = new HashMap<>();
      if (!(orgList.isEmpty())) {
        esMap = orgList.get(0);
         String contactDetails = (String)esMap.get(JsonKey.CONTACT_DETAILS);
         if(!ProjectUtil.isStringNullOREmpty(contactDetails)) {
           ObjectMapper mapper = new ObjectMapper();
           Object[] arr ;
          try {
            arr = mapper.readValue(contactDetails, Object[].class);
            esMap.put(JsonKey.CONTACT_DETAILS, arr);
          } catch (IOException e) {
            esMap.put(JsonKey.CONTACT_DETAILS, new Object[]{});
            ProjectLogger.log(e.getMessage(), e);
          } 
         }else {
           esMap.put(JsonKey.CONTACT_DETAILS, new Object[]{}); 
         }
      }
      // Register the org into EKStep.
      String hashOrgId = (String)esMap.getOrDefault(JsonKey.HASH_TAG_ID,"");
      ProjectLogger.log("hashOrgId value is ==" + hashOrgId);
      //Just check it if hashOrgId is null or empty then replace with org id.
      if(ProjectUtil.isStringNullOREmpty(hashOrgId)) {
        hashOrgId = id;
      }
      //making call to register tag
      registertag(hashOrgId, "{}", CourseBatchSchedulerUtil.headerMap);
      insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(), id, esMap);

    }
  }
 
  @SuppressWarnings("unchecked")
  private void updateOrgInfoToEs(Response actorMessage) {
    Map<String, Object> orgMap = (Map<String, Object>) actorMessage.get(JsonKey.ORGANISATION);
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.organisation.getTypeName(),
        (String) orgMap.get(JsonKey.ID), orgMap);
  }

  private boolean updateDataToElastic(String indexName, String typeName, String identifier,
      Map<String, Object> data) {
    boolean response = ElasticSearchUtil.updateData(indexName, typeName, identifier, data);
    if (response) {
      return true;
    }
    ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier,
        LoggerEnum.INFO.name());
    return false;

  }

  private void updateUserInfoToEs(Response actorMessage) {
    String userId = (String) actorMessage.get(JsonKey.ID);
    getUserProfile(userId);
  }

  @SuppressWarnings("unchecked")
  private void getUserProfile(String userId) {
    ProjectLogger.log("get user profile method call started user Id : " + userId);
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    EncryptionService service = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(null);
    DecryptionService decService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(null);
    Response response = null;
    List<Map<String, Object>> list = null;
    try {
      response = cassandraOperation
          .getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userId);
      list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      ProjectLogger
          .log("collecting user data to save user id : " + userId, LoggerEnum.INFO.name());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    if (!(list.isEmpty())) {
      Map<String, Object> map = list.get(0);
      Response addrResponse;
      list = null;
      try {
        ProjectLogger.log("collecting user address operation user Id : " + userId);
        String encUserId = service.encryptData(userId);
        addrResponse = cassandraOperation
            .getRecordsByProperty(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
                JsonKey.USER_ID, encUserId);
        list = (List<Map<String, Object>>) addrResponse.getResult().get(JsonKey.RESPONSE);
        ProjectLogger.log("collecting user address operation completed user Id : " + userId);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<>();
        }
      }
      map.put(JsonKey.ADDRESS, list);
      list = null;
      Response eduResponse = null;
      try {
        eduResponse = cassandraOperation
            .getRecordsByProperty(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        list = (List<Map<String, Object>>) eduResponse.getResult().get(JsonKey.RESPONSE);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<>();
        }
      }
      for (Map<String, Object> eduMap : list) {
        String addressId = (String) eduMap.get(JsonKey.ADDRESS_ID);
        if (!ProjectUtil.isStringNullOREmpty(addressId)) {

          Response addrResponseMap;
          List<Map<String, Object>> addrList = null;
          try {
            addrResponseMap = cassandraOperation
                .getRecordById(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
            addrList = (List<Map<String, Object>>) addrResponseMap.getResult()
                .get(JsonKey.RESPONSE);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          } finally {
            if (null == addrList) {
              addrList = new ArrayList<>();
            }
          }
          eduMap.put(JsonKey.ADDRESS, addrList.get(0));
        }
      }
      map.put(JsonKey.EDUCATION, list);

      Response jobProfileResponse;
      list = null;
      try {
        ProjectLogger.log("collecting user jobprofile user Id : " + userId);
        jobProfileResponse = cassandraOperation
            .getRecordsByProperty(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        list = (List<Map<String, Object>>) jobProfileResponse.getResult().get(JsonKey.RESPONSE);
        ProjectLogger.log("collecting user jobprofile collection completed userId : " + userId);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<>();
        }
      }
      for (Map<String, Object> eduMap : list) {
        String addressId = (String) eduMap.get(JsonKey.ADDRESS_ID);
        if (!ProjectUtil.isStringNullOREmpty(addressId)) {
          Response addrResponseMap;
          List<Map<String, Object>> addrList = null;
          try {
            addrResponseMap = cassandraOperation
                .getRecordById(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
            addrList = (List<Map<String, Object>>) addrResponseMap.getResult()
                .get(JsonKey.RESPONSE);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          } finally {
            if (null == addrList) {
              addrList = new ArrayList<>();
            }
          }
          eduMap.put(JsonKey.ADDRESS, addrList.get(0));
        }
      }
      map.put(JsonKey.JOB_PROFILE, list);
      list = null;
      List<Map<String, Object>> organisations = new ArrayList<>();
      try {
        Map<String, Object> reqMap = new HashMap<>();
         reqMap.put(JsonKey.USER_ID, userId);
         reqMap.put(JsonKey.IS_DELETED, false);
        Util.DbInfo orgUsrDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
         Response result = cassandraOperation
             .getRecordsByProperties(orgUsrDbInfo.getKeySpace(), orgUsrDbInfo.getTableName(), reqMap);
          list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
         Map<String, Object> orgDb = null;
         if (!(list.isEmpty())) {
           for (Map<String, Object> tempMap : list) {
             Map<String, Object> orgData = new HashMap<>();
             orgDb = (Map<String, Object>) tempMap;
             orgData.put(JsonKey.ORGANISATION_ID, orgDb.get(JsonKey.ORGANISATION_ID));
             orgData.put(JsonKey.ROLES, orgDb.get(JsonKey.ROLES));
             organisations.add(orgData);
           }
         }
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      map.put(JsonKey.ORGANISATIONS, organisations); 
      Util.removeAttributes(map, Arrays.asList(JsonKey.PASSWORD));
    } else {
      ProjectLogger
          .log("User data not found to save to ES user Id : " + userId, LoggerEnum.INFO.name());
    }
    if (!(((List<Map<String, String>>) response.getResult().get(JsonKey.RESPONSE)).isEmpty())) {
      ProjectLogger.log("saving started user to es userId : " + userId, LoggerEnum.INFO.name());
      Map<String, Object> map = ((List<Map<String, Object>>) response.getResult()
          .get(JsonKey.RESPONSE)).get(0);
      
      //save masked email and phone number
      DataMaskingService maskingService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getMaskingServiceInstance(null);
     String phone = (String)map.get(JsonKey.PHONE);
     String email = (String)map.get(JsonKey.EMAIL);
     
     
      if(!ProjectUtil.isStringNullOREmpty(phone)){
        map.put(JsonKey.ENC_PHONE, phone);
        map.put(JsonKey.PHONE, maskingService.maskPhone(decService.decryptData(phone)));
      }
      if(!ProjectUtil.isStringNullOREmpty(email)){
        map.put(JsonKey.ENC_EMAIL, email);
        map.put(JsonKey.EMAIL, maskingService.maskEmail(decService.decryptData(email)));
      }
      
      insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(),
          userId, map);
      ProjectLogger.log("saving completed user to es userId : " + userId);
    } else {
      ProjectLogger.log("user data not found to save to ES userId : " + userId);
    }

  }

  /**
   * Method to update the user count .
   */
  @SuppressWarnings("unchecked")
  private void updateUserCount(Response actorMessage) {
    String courseId = (String) actorMessage.get(JsonKey.COURSE_ID);
    Map<String, Object> updateRequestMap = actorMessage.getResult();

    Response result = cassandraOperation
        .getPropertiesValueById(dbInfo.getKeySpace(), dbInfo.getTableName(), courseId,
            JsonKey.USER_COUNT);
    Map<String, Object> responseMap = null;
    if (null != (result.get(JsonKey.RESPONSE))
        && (!((List<Map<String, Object>>) result.get(JsonKey.RESPONSE)).isEmpty())) {
      responseMap = ((List<Map<String, Object>>) result.get(JsonKey.RESPONSE)).get(0);
    }
    int userCount = (int) (responseMap.get(JsonKey.USER_COUNT) != null ? responseMap
        .get(JsonKey.USER_COUNT) : 0);
    updateRequestMap.put(JsonKey.USER_COUNT, userCount + 1);
    updateRequestMap.put(JsonKey.ID, courseId);
    updateRequestMap.remove(JsonKey.OPERATION);
    updateRequestMap.remove(JsonKey.COURSE_ID);
    Response resposne = cassandraOperation
        .updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), updateRequestMap);
    if (resposne.get(JsonKey.RESPONSE).equals(JsonKey.SUCCESS)) {
      ProjectLogger.log("USER COUNT UPDATED SUCCESSFULLY IN COURSE MGMT TABLE");
    } else {
      ProjectLogger.log("USER COUNT NOT UPDATED SUCCESSFULLY IN COURSE MGMT TABLE");
    }

  }

  /**
   * @param data Map<String, Object>
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean manageBackgroundJob(Map<String, Object> data) {
    if (data == null) {
      return false;
    }
    List<Map<String, Object>> list = (List<Map<String, Object>>) data.get(JsonKey.RESPONSE);
    Map<String, Object> content = list.get(0);
    String contentId = (String) content.get(JsonKey.CONTENT_ID);
    if (!ProjectUtil.isStringNullOREmpty(contentId)) {
      String contentData = getCourseData(contentId);
      if (!ProjectUtil.isStringNullOREmpty(contentData)) {
        Map<String, Object> map = getContentDetails(contentData);
        map.put(JsonKey.ID, (String) content.get(JsonKey.COURSE_ID));
        updateCourseManagement(map);
        List<String> createdForValue = null;
        Object obj = content.get(JsonKey.COURSE_CREATED_FOR);
        if (obj != null) {
          createdForValue = (List<String>) obj;
        }
        content.remove(JsonKey.COURSE_CREATED_FOR);
        content.put(JsonKey.APPLICABLE_FOR, createdForValue);
        Map<String, Object> finalResponseMap = (Map<String, Object>) map.get(JsonKey.RESULT);
        finalResponseMap.putAll(content);
        finalResponseMap.put(JsonKey.OBJECT_TYPE, ProjectUtil.EsType.course.getTypeName());
        insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            (String) map.get(JsonKey.ID), finalResponseMap);
      }
    }
    return true;
  }

  /**
   * Method to get the course data.
   *
   * @param contnetId String
   * @return String
   */
  private String getCourseData(String contnetId) {
    String responseData = null;
    try {
      String ekStepBaseUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
      if(ProjectUtil.isStringNullOREmpty(ekStepBaseUrl)) {
        ekStepBaseUrl = PropertiesCache.getInstance()
            .getProperty(JsonKey.EKSTEP_BASE_URL);
      }
      
      responseData = HttpUtil.sendGetRequest(ekStepBaseUrl+
          PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_URL) + contnetId,
          headerMap);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return responseData;
  }

  /**
   * Method to get the content details of the given content id.
   *
   * @param content String
   * @return Map<String, Object>
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getContentDetails(String content) {
    Map<String, Object> map = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JSONObject object = new JSONObject(content);
      JSONObject resultObj = object.getJSONObject(JsonKey.RESULT);
      HashMap<String, Map<String, Object>> result =
          mapper.readValue(resultObj.toString(), HashMap.class);
      Map<String, Object> contentMap = result.get(JsonKey.CONTENT);
      map.put(JsonKey.APPICON, contentMap.get(JsonKey.APPICON));
      try {
        map.put(JsonKey.TOC_URL, contentMap.get(JsonKey.TOC_URL));
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      map.put(JsonKey.COUNT, contentMap.get(JsonKey.LEAF_NODE_COUNT));
      map.put(JsonKey.RESULT, contentMap);

    } catch (JSONException | IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return map;
  }

  /**
   * Method to update the course management data on basis of course id.
   *
   * @param data Map<String, Object>
   * @return boolean
   */
  private boolean updateCourseManagement(Map<String, Object> data) {
    Map<String, Object> updateRequestMap = new HashMap<>();
    updateRequestMap
        .put(JsonKey.NO_OF_LECTURES, data.get(JsonKey.COUNT) != null ? data.get(JsonKey.COUNT) : 0);
    updateRequestMap.put(JsonKey.COURSE_LOGO_URL,
        data.get(JsonKey.APPICON) != null ? data.get(JsonKey.APPICON) : "");
    updateRequestMap
        .put(JsonKey.TOC_URL, data.get(JsonKey.TOC_URL) != null ? data.get(JsonKey.TOC_URL) : "");
    updateRequestMap.put(JsonKey.ID, data.get(JsonKey.ID));
    Response resposne = cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(),
        updateRequestMap);
    ProjectLogger.log(resposne.toString());
    if(!(resposne.get(JsonKey.RESPONSE) instanceof ProjectCommonException)) {
      return true;
    }
    return false;
  }

  /**
   * Method to cache the course data .
   *
   * @param index String
   * @param type String
   * @param identifier String
   * @param data Map<String,Object>
   * @return boolean
   */
  private boolean insertDataToElastic(String index, String type, String identifier,
      Map<String, Object> data) {
    ProjectLogger.log("making call to ES for type ,identifier ,data==" + type +" " + identifier + data);
    String response = ElasticSearchUtil.createData(index, type, identifier, data);
    ProjectLogger.log("Getting ES save response for type , identiofier==" +type+"  " + identifier + "  "+ response);
    if (!ProjectUtil.isStringNullOREmpty(response)) {
      ProjectLogger.log("User Data is saved successfully ES ." + type+ "  " + identifier);
      return true;
    }
    ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier,
        LoggerEnum.INFO.name());
    return false;
  }
  
  /**
   * This method will make EkStep api call register the tag.
   * @param tagId String unique tag id.
   * @param body  String requested body
   * @param header Map<String,String>
   * @return String
   */
  private String registertag(String tagId,String body, Map<String,String> header) {
    String tagStatus = "";
    try {
      ProjectLogger
          .log("start call for registering the tag ==" + tagId);
      tagStatus = HttpUtil.sendPostRequest(
          PropertiesCache.getInstance()
              .getProperty(JsonKey.EKSTEP_BASE_URL)
              + PropertiesCache.getInstance()
                  .getProperty(JsonKey.EKSTEP_TAG_API_URL)
              + "/" + tagId,
              body, header);
      ProjectLogger
          .log("end call for tag registration id and status  ==" + tagId
              + " " + tagStatus);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    return tagStatus;
  }
  
  @SuppressWarnings("unchecked")
  private void insertUserNotesToEs(Response actorMessage) {
    ProjectLogger.log("Calling method to save inside Es==");
    Map<String, Object> noteMap =
        (Map<String, Object>) actorMessage.get(JsonKey.NOTE);
    if (ProjectUtil.isNotNull(noteMap)) {
      String id = (String) noteMap.get(JsonKey.ID);
      insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.usernotes.getTypeName(), id, noteMap);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void updateUserNotesToEs(Response actorMessage) {
    Map<String, Object> noteMap = (Map<String, Object>) actorMessage.get(JsonKey.NOTE);
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.usernotes.getTypeName(),
        (String) noteMap.get(JsonKey.ID), noteMap);
  }
  
}
