package org.sunbird.learner.actors.syncjobmanager;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
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
import org.sunbird.learner.util.Util.DbInfo;

public class EsSyncActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  
  @Override
  public void onReceive(Object message) throws Throwable {
    ProjectLogger.log("EsSyncBackgroundJobManager  onReceive called");
    if (message instanceof Request) {
      try{
        Request actorMessage = (Request) message;
        String requestedOperation = actorMessage.getOperation();
        ProjectLogger.log("Operation name is ==" + requestedOperation);
        if (requestedOperation.equalsIgnoreCase(ActorOperations.SYNC.getValue())) {
          //return SUCCESS to controller and run the sync process in background
          Response response = new Response();
          response.put(JsonKey.SUCCESS, response);
          sender().tell(response, self());
          syncData(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
          ProjectLogger.log("Unsupported operation in Es sync Background Job Manager", exception);
        }
      
      }catch(Exception ex){
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE FOR BACKGROUND JOB MANAGER");
    }
    }

  private void syncData(Request message) {
    long startTime = System.currentTimeMillis();
    Map<String, Object> req = message.getRequest();
    Map<String ,Object> responseMap = new HashMap<>();
    List<Map<String,Object>> reponseList = null;
    List<Map<String,Object>> result = new ArrayList<>();
    Map<String, Object> dataMap = (Map<String, Object>) req.get(JsonKey.DATA);
    String objectType = (String) dataMap.get(JsonKey.OBJECT_TYPE);
    List<String> objectIds = null;
    if(dataMap.containsKey(JsonKey.OBJECT_IDS) && null != dataMap.get(JsonKey.OBJECT_IDS)){
      objectIds = (List<String>) dataMap.get(JsonKey.OBJECT_IDS);
    }
    Util.DbInfo dbInfo = getDbInfoObj(objectType);
    if(null != objectIds && !objectIds.isEmpty()){
      Response response = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(), dbInfo.getTableName(), JsonKey.ID, objectIds);
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    }
    if(null != reponseList  && !reponseList.isEmpty()){
      for(Map<String, Object> map : reponseList){
        responseMap.put((String)map.get(JsonKey.ID), map);
      }
    }else{
      Response response =  cassandraOperation.getAllRecords(dbInfo.getKeySpace(), dbInfo.getTableName());
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if(null != reponseList ){
        for(Map<String, Object> map : reponseList){
          responseMap.put((String)map.get(JsonKey.ID), map);
        }
      }
    }
    Iterator<Entry<String, Object>>  itr = responseMap.entrySet().iterator();
      while(itr.hasNext()){
        if(objectType.equals(JsonKey.USER)){
          result.add(getUserDetails(itr.next()));
        }else if (objectType.equals(JsonKey.ORGANISATION)){
          result.add(getOrgDetails(itr.next()));
        }
      }
    
    ElasticSearchUtil.bulkInsertData(ProjectUtil.EsIndex.sunbird.getIndexName(), getType(objectType), result);
    long stopTime = System.currentTimeMillis();
    long elapsedTime = stopTime - startTime;
    ProjectLogger.log("total time taken to sync db data for "+ objectType +" to Elastic search "+ elapsedTime);
  }

  private String getType(String objectType) {
    String type = "";
    if(objectType.equals(JsonKey.USER)){
      type = ProjectUtil.EsType.user.getTypeName();
    }else if(objectType.equals(JsonKey.ORGANISATION)){
      type = ProjectUtil.EsType.organisation.getTypeName();
    }
    return type;
  }

  private Map<String, Object> getOrgDetails(Entry<String, Object> entry) {
    String orgId = entry.getKey();
    Map<String,Object> orgMap = (Map<String, Object>) entry.getValue();
    if(orgMap.containsKey(JsonKey.ADDRESS_ID) && !ProjectUtil.isStringNullOREmpty((String)orgMap.get(JsonKey.ADDRESS_ID))){
      orgMap.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),(String)orgMap.get(JsonKey.ADDRESS_ID)));
    }
    return orgMap;
  }

  private Map<String, Object> getUserDetails(Entry<String, Object> entry) {
    String userId = entry.getKey();
    Map<String,Object> userMap = (Map<String, Object>) entry.getValue();
    Util.removeAttributes(userMap, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY, JsonKey.ID));
    userMap.put(JsonKey.ADDRESS, getDetails(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),userId,JsonKey.USER_ID));
    List<Map<String, Object>> eduMap = getDetails(Util.dbInfoMap.get(JsonKey.EDUCATION_DB),userId,JsonKey.USER_ID);
    for(Map<String, Object> map : eduMap){
      if(map.containsKey(JsonKey.ADDRESS_ID) && !ProjectUtil.isStringNullOREmpty((String)map.get(JsonKey.ADDRESS_ID))){
        map.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),userId));
      }
    }
    userMap.put(JsonKey.EDUCATION, eduMap);
    List<Map<String, Object>> jobMap = getDetails(Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB),userId,JsonKey.USER_ID);
    for(Map<String, Object> map : jobMap){
      if(map.containsKey(JsonKey.ADDRESS_ID) && !ProjectUtil.isStringNullOREmpty((String)map.get(JsonKey.ADDRESS_ID))){
        map.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),userId));
      }
    }
    userMap.put(JsonKey.JOB_PROFILE, jobMap);
    List<Map<String, Object>> orgMap = getDetails(Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB),userId,JsonKey.USER_ID);
    List<Map<String, Object>> organisations = new ArrayList<>();
    Map<String, Object> orgDb = null;
    for(Map<String, Object> map : orgMap){
        Map<String, Object> orgData = new HashMap<>();
        orgDb =  map;
        orgData.put(JsonKey.ORGANISATION_ID, orgDb.get(JsonKey.ORGANISATION_ID));
        orgData.put(JsonKey.ROLES, orgDb.get(JsonKey.ROLES));
        organisations.add(orgData);
    }
    userMap.put(JsonKey.ORGANISATIONS, organisations);
    return userMap;
  }
  
  

  private Object getDetailsById(DbInfo dbInfo, String userId) {
    try{
      Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(),userId);
      return ((((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).isEmpty()) ?  new HashMap<>(): ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).get(0));
    }catch(Exception ex){
      
    }
    return null;
  }

  private List<Map<String,Object>> getDetails(DbInfo dbInfo, String userId, String property) {
    try{
      Response response = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(), dbInfo.getTableName(), property, userId);
      return (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    }catch(Exception ex){
      
    }
    return null;
  }

  private DbInfo getDbInfoObj(String objectType) {
    if(objectType.equals(JsonKey.USER)){
      return Util.dbInfoMap.get(JsonKey.USER_DB);
    }else if (objectType.equals(JsonKey.ORGANISATION)){
      return Util.dbInfoMap.get(JsonKey.ORG_DB);
    }
    return null;
  }
 }


