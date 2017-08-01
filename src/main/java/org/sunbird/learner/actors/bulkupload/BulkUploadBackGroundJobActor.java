package org.sunbird.learner.actors.bulkupload;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;

public class BulkUploadBackGroundJobActor extends UntypedAbstractActor {

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
    }else {
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
  private void process(Request actorMessage) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    List<String[]> dataList = (List<String[]>) actorMessage.get(JsonKey.DATA);
    if(((String)actorMessage.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.USER)){
      String[] columnArr = dataList.get(0);
      List<Map<String, Object>> badUserReq = new ArrayList<>();
      Map<String,Object> userMap = null;
      for(int i = 1 ; i < dataList.size() ; i++){
        userMap = new HashMap<>();
        String[] valueArr = dataList.get(i);
        for(int j = 0 ; j < valueArr.length ; j++){
            userMap.put(columnArr[j], valueArr[j]);
         }
        if(validateUser(userMap)){
          try{
            userMap = insertRecordToKeyCloak(userMap);
            Response response = null;
            try {
              response = cassandraOperation
                  .insertRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
            } finally {
              if (null == response) {
                ssoManager.removeUser(userMap);
              }
            }
          }catch(Exception ex){
            ProjectLogger.log("Exception occurred while bulk user upload :", ex);
            userMap.remove(JsonKey.ID);
            badUserReq.add(userMap);
          }
        }else{
          badUserReq.add(userMap);
        }
       }
      //Insert record to BulkDb table
      //String processId = (String) actorMessage.get(JsonKey.PROCESS_ID);
     }
   }
  

  @SuppressWarnings("unchecked")
  private Map<String, Object> insertRecordToKeyCloak(Map<String, Object> userMap) {
    
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    
    if (userMap.containsKey(JsonKey.PROVIDER) && !ProjectUtil.isStringNullOREmpty((String)userMap.get(JsonKey.PROVIDER))) {
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
        ProjectLogger.log(exception.getMessage(), exception);
        throw exception;
      }
      
      userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
      if (ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.ROOT_ORG_ID))) {
        userMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
      }
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

  private boolean validateUser(Map<String,Object> map) {
    if (map.get(JsonKey.USERNAME) == null) {
        return false;
    }
    if (map.get(JsonKey.FIRST_NAME) == null
            || (ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.FIRST_NAME)))) {
      return false;
    }  
    if (map.get(JsonKey.EMAIL) == null) {
      return false;
    }
    if (!ProjectUtil.isEmailvalid((String) map.get(JsonKey.EMAIL))) {
      return false;
    }
    if(!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PROVIDER))){
      if(!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE))){
          if(null != map.get(JsonKey.PHONE_NUMBER_VERIFIED)){
            if(map.get(JsonKey.PHONE_NUMBER_VERIFIED) instanceof Boolean){
              if(!((boolean) map.get(JsonKey.PHONE_NUMBER_VERIFIED))){
                return false;
              }
            }else{
              return false;
            }
          }else{
            return false;
          }
        }
      if(null != map.get(JsonKey.EMAIL_VERIFIED)){
        if(map.get(JsonKey.EMAIL_VERIFIED) instanceof Boolean){
          if(!((boolean) map.get(JsonKey.EMAIL_VERIFIED))){
            return false;
          }
        }else{
          return false;
        }
      }else{
        return false;
      } 
    }
    return true;
  }

}
