package org.sunbird.learner.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 */
public class UserManagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  
  private ActorRef backGroundActorRef;

  public UserManagementActor() {
    backGroundActorRef = getContext().actorOf(Props.create(BackgroundJobManager.class), "backGroundActor");
   }

  /**
   * Receives the actor message and perform the course enrollment operation .
   */
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("UserManagementActor  onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_USER.getValue())) {
          createUser(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_USER.getValue())) {
          updateUser(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.LOGIN.getValue())) {
          login(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.LOGOUT.getValue())) {
          logout(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.CHANGE_PASSWORD.getValue())) {
          changePassword(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_PROFILE.getValue())) {
          getUserProfile(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_ROLES.getValue())) {
          getRoles(); 
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.JOIN_USER_ORGANISATION.getValue())) {
          joinUserOrganisation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.APPROVE_USER_ORGANISATION.getValue())) {
          approveUserOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue())) {
          getUserDetailsByLoginId(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
          rejectUserOrg(actorMessage);
        }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.DOWNLOAD_USERS.getValue())) {
           getUserDetails(actorMessage);
        }else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.BLOCK_USER.getValue())) {
          blockUser(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ASSIGN_ROLES.getValue())) {
            assignRoles(actorMessage);
        }else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UNBLOCK_USER.getValue())) {
          unBlockUser(actorMessage);
        }
        else {
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
    } 
    }


 

  @SuppressWarnings("unchecked")
  private void getUserDetailsByLoginId(Request actorMessage) {
    
  Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
  if(null != userMap.get(JsonKey.LOGIN_ID)){
    String loginId = (String)userMap.get(JsonKey.LOGIN_ID);
    Response resultFrLoginId = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),
        JsonKey.LOGIN_ID,loginId);
    if(!((List<Map<String,Object>>)resultFrLoginId.get(JsonKey.RESPONSE)).isEmpty()){
      Map<String,Object> map = ((List<Map<String,Object>>)resultFrLoginId.get(JsonKey.RESPONSE)).get(0);
      Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), 
          ProjectUtil.EsType.user.getTypeName(), (String)map.get(JsonKey.USER_ID));
       
      if (result == null || result.size()==0) {
        throw new ProjectCommonException(
            ResponseCode.userNotFound.getErrorCode(),
            ResponseCode.userNotFound.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      
      //check whether is_deletd true or false
      if(ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED) && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))&&(Boolean)result.get(JsonKey.IS_DELETED)){
        throw new ProjectCommonException(
            ResponseCode.userAccountlocked.getErrorCode(),
            ResponseCode.userAccountlocked.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
        fetchRootAndRegisterOrganisation(result);
        //having check for removing private filed from user , if call user and response 
        //user data id is not same.
        String requestedById = (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY,"");
        ProjectLogger.log("requested By and requested user id == " + requestedById +"  " + (String)map.get(JsonKey.USER_ID));
        if(!(((String)map.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById))){
           result = removeUserPrivateField(result);
        }
        Response response = new Response();
        if (null != result) {
          response.put(JsonKey.RESPONSE, result);
        } else {
          result = new HashMap<>();
          response.put(JsonKey.RESPONSE, result);
        }
       sender().tell(response, self());
      return;
    }else{
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(), 
          ResponseCode.userNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }
  }
}

  private void fetchRootAndRegisterOrganisation(Map<String, Object> result) {
    try {
      if (isNotNull(result.get(JsonKey.ROOT_ORG_ID))) {

        String rootOrgId = (String) result.get(JsonKey.ROOT_ORG_ID);
        Map<String, Object> esResult = ElasticSearchUtil
            .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName(), rootOrgId);
        result.put(JsonKey.ROOT_ORG, esResult);

      }
      if (isNotNull(result.get(JsonKey.REGISTERED_ORG_ID))) {

        String regOrgId = (String) result.get(JsonKey.REGISTERED_ORG_ID);
        Map<String, Object> esResult = ElasticSearchUtil
            .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName(), regOrgId);
        result.put(JsonKey.REGISTERED_ORG, esResult != null ? esResult : new HashMap<>());
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  /**
   * Method to get the user profile .
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void getUserProfile(Request actorMessage) {
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    Map<String, Object> result = ElasticSearchUtil
        .getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), (String) userMap.get(JsonKey.USER_ID));
    //check user found or not
    if (result == null || result.size() == 0) {
      throw new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    //check whether is_deletd true or false
    if(ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED) && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))&&(Boolean)result.get(JsonKey.IS_DELETED)){
      throw new ProjectCommonException(
          ResponseCode.userAccountlocked.getErrorCode(),
          ResponseCode.userAccountlocked.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    fetchRootAndRegisterOrganisation(result);
    //having check for removing private filed from user , if call user and response 
    //user data id is not same.
    String requestedById = (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY,"");
    ProjectLogger.log("requested By and requested user id == " + requestedById +"  " + (String) userMap.get(JsonKey.USER_ID));
    if(!((String) userMap.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById)){
       result = removeUserPrivateField(result);
    }
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
   * Gets the organisation details for the given user Id from cassandra
   *
   * @return List<Map<String,Object>>
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getOrganisationDetailsByUserId(String userId) {
    List<Map<String, Object>> organisations = new ArrayList<>();

    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.USER_ID, userId);
    reqMap.put(JsonKey.IS_DELETED, false);

    Util.DbInfo orgUsrDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Response result = cassandraOperation
        .getRecordsByProperties(orgUsrDbInfo.getKeySpace(), orgUsrDbInfo.getTableName(), reqMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    Map<String, Object> orgDb = null;
    if (!(list.isEmpty())) {
      for (Map<String, Object> map : list) {
        Map<String, Object> orgData = new HashMap<>();
        orgDb = (Map<String, Object>) map;
        orgData.put(JsonKey.ORGANISATION_ID, orgDb.get(JsonKey.ORGANISATION_ID));
        orgData.put(JsonKey.ROLES, orgDb.get(JsonKey.ROLES));
        organisations.add(orgData);
      }
    }
    return organisations;
  }

  /**
   * Method to change the user password .
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void changePassword(Request actorMessage) {
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    String currentPassword = (String) userMap.get(JsonKey.PASSWORD);
    String newPassword = (String) userMap.get(JsonKey.NEW_PASSWORD);
    Response result = cassandraOperation
        .getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(),
            (String) userMap.get(JsonKey.USER_ID));
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      Map<String, Object> resultMap = list.get(0);
      boolean passwordMatched = ((String) resultMap.get(JsonKey.PASSWORD))
          .equals(OneWayHashing.encryptVal(currentPassword));
      if (passwordMatched) {
        // update the new password
        String newHashedPassword = OneWayHashing.encryptVal(newPassword);
        Map<String, Object> queryMap = new LinkedHashMap<>();
        queryMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
        queryMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        queryMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
        queryMap.put(JsonKey.PASSWORD, newHashedPassword);
        result = cassandraOperation
            .updateRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), queryMap);
        sender().tell(result, self());
      } else {
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.invalidCredentials.getErrorCode(),
            ResponseCode.invalidCredentials.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
    }
  }

  /**
   * Method to Logout the user , once logout successfully it will delete the AuthToken from DB.
   *
   * @param actorMessage Request
   */
  private void logout(Request actorMessage) {
    Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
    String authToken = (String) actorMessage.getRequest().get(JsonKey.AUTH_TOKEN);

    Response result = cassandraOperation
        .deleteRecord(userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(), authToken);

    result.put(Constants.RESPONSE, JsonKey.SUCCESS);
    sender().tell(result, self());
  }

  /**
   * Method to Login the user by taking Username and password , once login successful it will create
   * Authtoken and return the token. user can login from multiple source at a time for example web,
   * app,etc but for a same source user cann't be login from different machine, for example if user
   * is trying to login from a source from two different machine we are invalidating the auth token
   * of previous machine and creating a new auth token for new machine and sending that auth token
   * with response
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void login(Request actorMessage) {
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> reqMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    Response result = cassandraOperation
        .getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(),
            OneWayHashing.encryptVal((String) reqMap.get(JsonKey.USERNAME)));
    List<Map<String, Object>> list = ((List<Map<String, Object>>) result.get(JsonKey.RESPONSE));
    if (null != list && list.size() == 1) {
      Map<String, Object> resultMap = list.get(0);
      if (null != resultMap.get(JsonKey.STATUS) &&
          (ProjectUtil.Status.ACTIVE.getValue()) == (int) resultMap.get(JsonKey.STATUS)) {
        if (ProjectUtil.isStringNullOREmpty(((String) reqMap.get(JsonKey.LOGIN_TYPE)))) {
          //here login type is general
          boolean password = ((String) resultMap.get(JsonKey.PASSWORD))
              .equals(OneWayHashing.encryptVal(
                  (String) reqMap.get(JsonKey.PASSWORD)));
          if (password) {
            Map<String, Object> userAuthMap = new HashMap<>();
            userAuthMap.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
            userAuthMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.ID));
            userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

            String userAuth = ProjectUtil.createUserAuthToken((String) resultMap.get(JsonKey.ID),
                (String) reqMap.get(JsonKey.SOURCE));
            userAuthMap.put(JsonKey.ID, userAuth);
            checkForDuplicateUserAuthToken(userAuthMap, resultMap, reqMap);

            Map<String, Object> user = new HashMap<>();
            user.put(JsonKey.ID, OneWayHashing.encryptVal((String) reqMap.get(JsonKey.USERNAME)));
            user.put(JsonKey.LAST_LOGIN_TIME, ProjectUtil.getFormattedDate());

            cassandraOperation
                .updateRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), user);

            reqMap.remove(JsonKey.PASSWORD);
            reqMap.remove(JsonKey.USERNAME);
            reqMap.put(JsonKey.FIRST_NAME, resultMap.get(JsonKey.FIRST_NAME));
            reqMap.put(JsonKey.TOKEN, userAuthMap.get(JsonKey.ID));
            reqMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
            Response response = new Response();
            response.put(Constants.RESPONSE, reqMap);
            sender().tell(response, self());
          } else {
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidCredentials.getErrorCode(),
                ResponseCode.invalidCredentials.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
          }
        } else {
          //for other login type operation
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.loginTypeError.getErrorCode(),
              ResponseCode.loginTypeError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } else {
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.invalidCredentials.getErrorCode(),
            ResponseCode.invalidCredentials.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
    } else {
      //TODO:need to implement code for other login like fb login, gmail login etc
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidCredentials.getErrorCode(),
          ResponseCode.invalidCredentials.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  /**
   * Method to update the user profile.
   */
  @SuppressWarnings("unchecked")
  private void updateUser(Request actorMessage) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> requestMap = null;
    Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);
    if (null != userMap.get(JsonKey.USER_ID)) {
      userMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
    }else{
      userMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    if (null != userMap.get(JsonKey.EMAIL)) {
      checkForEmailAndUserNameUniqueness(userMap, usrDbInfo);
    }
    //not allowing user to update the status,provider,userName
    userMap.remove(JsonKey.STATUS);
    userMap.remove(JsonKey.PROVIDER);
    userMap.remove(JsonKey.USERNAME);
    userMap.remove(JsonKey.REGISTERED_ORG_ID);
    userMap.remove(JsonKey.ROOT_ORG_ID);
       
    boolean isSSOEnabled = Boolean
        .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
    if (isSSOEnabled) {
      UpdateKeyCloakUserBase(userMap);
    }
    userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    userMap.put(JsonKey.UPDATED_BY, req.get(userMap.get(JsonKey.REQUESTED_BY)));
    requestMap = new HashMap<>();
    requestMap.putAll(userMap);
    removeUnwanted(requestMap);
    Response result = null;
    try {
      result = cassandraOperation
          .updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    } catch (Exception ex) {
      sender().tell(ex, self());
      return;
    }
    //update user address
    if (userMap.containsKey(JsonKey.ADDRESS)) {
      List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS);
      for (int i = 0; i < reqList.size(); i++) {
        Map<String, Object> reqMap = reqList.get(i);
        if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) &&
            ((boolean) reqMap.get(JsonKey.IS_DELETED))
            && !ProjectUtil.isStringNullOREmpty((String) reqMap.get(JsonKey.ID))) {
          deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
              (String) reqMap.get(JsonKey.ID));
          continue;
        }  
        processUserAddress(reqMap,req,userMap,addrDbInfo);

      }
    }
    if (userMap.containsKey(JsonKey.EDUCATION)) {
      List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap
          .get(JsonKey.EDUCATION);
      for (int i = 0; i < reqList.size(); i++) {
        Map<String, Object> reqMap = reqList.get(i);
        if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) &&
            ((boolean) reqMap.get(JsonKey.IS_DELETED))
            && !ProjectUtil.isStringNullOREmpty((String) reqMap.get(JsonKey.ID))) {
          String addrsId = null;
          if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
            addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
            deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
          } else {
            addrsId = getAddressId((String) reqMap.get(JsonKey.ID), eduDbInfo);
            if (null != addrsId) {
              deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
            }
          }
          deleteRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
              (String) reqMap.get(JsonKey.ID));
          continue;
        }
        processEducationInfo(reqMap, userMap, req, addrDbInfo, eduDbInfo);
      }
    }
    if (userMap.containsKey(JsonKey.JOB_PROFILE)) {
      List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap
          .get(JsonKey.JOB_PROFILE);
      for (Map<String, Object> reqMap : reqList) {
        if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) &&
            ((boolean) reqMap.get(JsonKey.IS_DELETED))
            && !ProjectUtil.isStringNullOREmpty((String) reqMap.get(JsonKey.ID))) {
          String addrsId = null;
          if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
            addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
            deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
          } else {
            addrsId = getAddressId((String) reqMap.get(JsonKey.ID), eduDbInfo);
            if (null != addrsId) {
              deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
            }
          }
          deleteRecord(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(),
              (String) reqMap.get(JsonKey.ID));
          continue;
        }
        processJobProfileInfo(reqMap, userMap, req, addrDbInfo, jobProDbInfo);
      }
    }

    updateUserExtId(requestMap, usrExtIdDb);
    sender().tell(result, self());

    if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      Response usrResponse = new Response();
      usrResponse.getResult()
          .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      usrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
      try {
        backGroundActorRef.tell(usrResponse,self());
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occured during saving user to Es while updating user : ", ex);
      }
    }
  }


  private void processUserAddress(Map<String, Object> reqMap, Map<String, Object> req, 
      Map<String, Object> userMap, DbInfo addrDbInfo) {
    if (!reqMap.containsKey(JsonKey.ID)) {
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    } else {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
    }
    try {
      cassandraOperation
          .upsertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), reqMap);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private String getAddressId(String id, DbInfo eduDbInfo) {
    String addressId = null;
    try {
      Response res = cassandraOperation
          .getPropertiesValueById(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
              id, JsonKey.ADDRESS_ID);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        addressId = (String) (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0))
            .get(JsonKey.ADDRESS_ID);
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
    return addressId;
  }

  private void deleteRecord(String keyspaceName, String tableName, String id) {
    try {
      cassandraOperation.deleteRecord(keyspaceName, tableName, id);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  private void processJobProfileInfo(Map<String, Object> reqMap, Map<String, Object> userMap,
      Map<String, Object> req,
      DbInfo addrDbInfo, DbInfo jobProDbInfo) {
    String addrId = null;
    Response addrResponse = null;
    if (reqMap.containsKey(JsonKey.ADDRESS)) {
      @SuppressWarnings("unchecked")
      Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
      if (!address.containsKey(JsonKey.ID)) {
        addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      } else {
        addrId = (String) address.get(JsonKey.ID);
        address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
      }
      try {
        addrResponse = cassandraOperation
            .upsertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }
    if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
        .equalsIgnoreCase(JsonKey.SUCCESS)) {
      reqMap.put(JsonKey.ADDRESS_ID, addrId);
      reqMap.remove(JsonKey.ADDRESS);
    }

    if (reqMap.containsKey(JsonKey.ID)) {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
    } else {
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    try {
      cassandraOperation
          .upsertRecord(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), reqMap);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  private void processEducationInfo(Map<String, Object> reqMap, Map<String, Object> userMap,
      Map<String, Object> req,
      DbInfo addrDbInfo, DbInfo eduDbInfo) {

    String addrId = null;
    Response addrResponse = null;
    if (reqMap.containsKey(JsonKey.ADDRESS)) {
      @SuppressWarnings("unchecked")
      Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
      if (!address.containsKey(JsonKey.ID)) {
        addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      } else {
        addrId = (String) address.get(JsonKey.ID);
        address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
      }
      try {
        addrResponse = cassandraOperation
            .upsertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }
    if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
        .equalsIgnoreCase(JsonKey.SUCCESS)) {
      reqMap.put(JsonKey.ADDRESS_ID, addrId);
      reqMap.remove(JsonKey.ADDRESS);
    }
    try {
      reqMap.put(JsonKey.YEAR_OF_PASSING,
          ((BigInteger) reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }

    if (null != reqMap.get(JsonKey.PERCENTAGE)) {
      reqMap.put(JsonKey.PERCENTAGE,
          Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
    }
    if (reqMap.containsKey(JsonKey.ID)) {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
    } else {
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    try {
      cassandraOperation.upsertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), reqMap);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }


  }

  @SuppressWarnings("unchecked")
  private void checkForEmailAndUserNameUniqueness(Map<String, Object> userMap, DbInfo usrDbInfo) {
    if (null != userMap.get(JsonKey.USERNAME)) {
      String userName = (String) userMap.get(JsonKey.USERNAME);
      Response resultFruserName = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),
          usrDbInfo.getTableName(), JsonKey.USERNAME, userName);
      if (!(((List<Map<String, Object>>) resultFruserName.get(JsonKey.RESPONSE)).isEmpty())) {
        Map<String, Object> dbusrMap = ((List<Map<String, Object>>) resultFruserName
            .get(JsonKey.RESPONSE)).get(0);
        String usrId = (String) dbusrMap.get(JsonKey.USER_ID);
        if (!(usrId.equals(userMap.get(JsonKey.ID)))) {
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.userNameAlreadyExistError.getErrorCode(),
              ResponseCode.userNameAlreadyExistError.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      }
    }
  }

  private void UpdateKeyCloakUserBase(Map<String, Object> userMap) {
    try {
      SSOManager ssoManager = new KeyCloakServiceImpl();
      String userId = ssoManager.updateUser(userMap);
      if (!(!ProjectUtil.isStringNullOREmpty(userId) && userId.equalsIgnoreCase(JsonKey.SUCCESS))) {
        throw new ProjectCommonException(
            ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
            ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      throw new ProjectCommonException(
          ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
          ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  /**
   * Method to create the new user , Username should be unique .
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void createUser(Request actorMessage) {
    ProjectLogger.log("create user method started..");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    Util.DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
    ProjectLogger.log("collected all the DB setup..");
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> requestMap = null;
    Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);

    boolean isSSOEnabled = Boolean  
        .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
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
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.userAlreadyExist.getErrorCode(),
            ResponseCode.userAlreadyExist.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
    }
    //validate root org and reg org
    userMap.put(JsonKey.ROOT_ORG_ID,JsonKey.DEFAULT_ROOT_ORG_ID);
    if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.REGISTERED_ORG_ID))) {
      Response orgResponse = null;
      try {
        orgResponse = cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(),
            (String) userMap.get(JsonKey.REGISTERED_ORG_ID));
      } catch (Exception e) {
        ProjectLogger.log("Exception occured while verifying regOrgId during create user : ", e);
        throw new ProjectCommonException(
            ResponseCode.invalidOrgId.getErrorCode(),
            ResponseCode.invalidOrgId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      List<Map<String,Object>> responseList = (List<Map<String, Object>>) orgResponse.get(JsonKey.RESPONSE);
      String rootOrgId = "";
      if(null != responseList && !(responseList.isEmpty())){
       String orgId = (String) responseList.get(0).get(JsonKey.ID);
       Map<String,Object> orgMap = responseList.get(0);
      boolean isRootOrg = false;
      if(null != orgMap.get(JsonKey.IS_ROOT_ORG)){
        isRootOrg = (boolean) orgMap.get(JsonKey.IS_ROOT_ORG);
      }else{
        isRootOrg = false;
      }
      if(isRootOrg){
        rootOrgId = orgId;
      }else{
        String channel = (String) orgMap.get(JsonKey.CHANNEL);
        if(!ProjectUtil.isStringNullOREmpty( channel)){
          Map<String,Object> filters = new HashMap<>();
          filters.put(JsonKey.CHANNEL, channel);
          filters.put(JsonKey.IS_ROOT_ORG, true);
          Map<String , Object> esResult = elasticSearchComplexSearch(filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
          if(isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT) && isNotNull(esResult.get(JsonKey.CONTENT)) && !(((List<String>)esResult.get(JsonKey.CONTENT)).isEmpty())){
              Map<String , Object> esContent = ((List<Map<String, Object>>)esResult.get(JsonKey.CONTENT)).get(0);
              rootOrgId =  (String) esContent.get(JsonKey.ID);
          }else{
            throw  new ProjectCommonException(
                ResponseCode.invalidRootOrgData.getErrorCode(),
                ProjectUtil.formatMessage(ResponseCode.invalidRootOrgData.getErrorMessage(),channel ),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }else{
          rootOrgId = JsonKey.DEFAULT_ROOT_ORG_ID;
        }
      }
      userMap.put(JsonKey.ROOT_ORG_ID,rootOrgId);
     }else{
       throw new ProjectCommonException(
           ResponseCode.invalidOrgId.getErrorCode(),
           ResponseCode.invalidOrgId.getErrorMessage(),
           ResponseCode.CLIENT_ERROR.getResponseCode());
     }
    //--------------------------------------------------
    }
    SSOManager ssoManager = new KeyCloakServiceImpl();
    if (isSSOEnabled) {
      try {
        String userId = ssoManager.createUser(userMap);
        if (!ProjectUtil.isStringNullOREmpty(userId)) {
          userMap.put(JsonKey.USER_ID, userId);
          userMap.put(JsonKey.ID, userId);
        } else {
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.userRegUnSuccessfull.getErrorCode(),
              ResponseCode.userRegUnSuccessfull.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      } catch (Exception exception) {
        ProjectLogger.log(exception.getMessage(), exception);
        sender().tell(exception, self());
        return;
      }
    } else {
      userMap
          .put(JsonKey.USER_ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
      userMap.put(JsonKey.ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
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
    ProjectLogger.log("User roles is===" + userMap.get(JsonKey.ROLES));
    requestMap = new HashMap<>();
    requestMap.putAll(userMap);
    removeUnwanted(requestMap);
    Response response = null;
    try {
      response = cassandraOperation
          .insertRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    } catch (ProjectCommonException exception) {
      sender().tell(exception, self());
      return;
    } finally {
      if (null == response && isSSOEnabled) {
        ssoManager.removeUser(userMap);
      }
    }
    response.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      if (userMap.containsKey(JsonKey.ADDRESS)) {
        List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap
            .get(JsonKey.ADDRESS);
        for (int i = 0; i < reqList.size(); i++) {
          Map<String, Object> reqMap = reqList.get(i);
          reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
          reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
          reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
          reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
          try {
            cassandraOperation
                .insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), reqMap);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          }
        }
      }
      ProjectLogger.log("User insertation on DB started--.....");
      if (userMap.containsKey(JsonKey.EDUCATION)) {
        insertEducationDetails(userMap, addrDbInfo, eduDbInfo);
        ProjectLogger.log("User insertation for Education done--.....");
      }
      if (userMap.containsKey(JsonKey.JOB_PROFILE)) {
        insertJobProfileDetails(userMap, addrDbInfo, jobProDbInfo);
        ProjectLogger.log("User insertation for Job profile done--.....");
      }
      if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.REGISTERED_ORG_ID))) {
        Response orgResponse = null;
        try {
          orgResponse = cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(),
              (String) userMap.get(JsonKey.REGISTERED_ORG_ID));
        } catch (Exception e) {
          ProjectLogger.log("Exception occured while verifying regOrgId during create user : ", e);
        }
        if (null != orgResponse && (!((List<Map<String, Object>>) orgResponse.get(JsonKey.RESPONSE))
            .isEmpty())) {
          insertOrganisationDetails(userMap, usrOrgDb);
        } else {
          ProjectLogger.log(
              "Reg Org Id :" + (String) userMap.get(JsonKey.REGISTERED_ORG_ID) + " for user id " +
                  userMap.get(JsonKey.ID) + " is not valid.");
        }
      }
      //update the user external identity data
      ProjectLogger.log("User insertation for extrenal identity started--.....");
      updateUserExtId(requestMap, usrExtIdDb);
      ProjectLogger.log("User insertation for extrenal identity completed--.....");
    }

    ProjectLogger.log("User created successfully.....");
    sender().tell(response, self());

   
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("method call going to satrt for ES--.....");
      Response usrResponse = new Response();
      usrResponse.getResult()
          .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      usrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
      ProjectLogger.log("making a call to save user data to ES");
      try {
        backGroundActorRef.tell(usrResponse,self());
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occured during saving user to Es while creating user : ", ex);
      }
    } else {
      ProjectLogger.log("no call for ES to save user");
    }

  }

  private void insertOrganisationDetails(Map<String, Object> userMap, DbInfo usrOrgDb) {

    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
    reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    reqMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
    reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    List<String> roleList = new ArrayList<>();
    roleList.add(ProjectUtil.UserRole.CONTENT_CREATOR.getValue());
    reqMap.put(JsonKey.ROLES, roleList);
    reqMap.put(JsonKey.IS_DELETED, false);

    try {
      cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(), reqMap);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  private void insertJobProfileDetails(Map<String, Object> userMap, DbInfo addrDbInfo,
      DbInfo jobProDbInfo) {

    List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap
        .get(JsonKey.JOB_PROFILE);
    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> reqMap = reqList.get(i);
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
      String addrId = null;
      Response addrResponse = null;
      if (reqMap.containsKey(JsonKey.ADDRESS)) {
        Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
        addrId = ProjectUtil.getUniqueIdFromTimestamp(i + 1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
        try {
          addrResponse = cassandraOperation
              .insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
        } catch (Exception e) {
          ProjectLogger.log(e.getMessage(), e);
        }
      }
      if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
          .equalsIgnoreCase(JsonKey.SUCCESS)) {
        reqMap.put(JsonKey.ADDRESS_ID, addrId);
        reqMap.remove(JsonKey.ADDRESS);
      }
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
      try {
        cassandraOperation
            .insertRecord(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), reqMap);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }


  }

  @SuppressWarnings("unchecked")
  private void insertEducationDetails(Map<String, Object> userMap, DbInfo addrDbInfo,
      DbInfo eduDbInfo) {

    List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION);
    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> reqMap = reqList.get(i);
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
      String addrId = null;
      Response addrResponse = null;
      if (reqMap.containsKey(JsonKey.ADDRESS)) {
        Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
        addrId = ProjectUtil.getUniqueIdFromTimestamp(i + 1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
        try {
          addrResponse = cassandraOperation
              .insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
        } catch (Exception e) {
          ProjectLogger.log(e.getMessage(), e);
        }
      }
      if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
          .equalsIgnoreCase(JsonKey.SUCCESS)) {
        reqMap.put(JsonKey.ADDRESS_ID, addrId);
        reqMap.remove(JsonKey.ADDRESS);
      }
      try {
        reqMap.put(JsonKey.YEAR_OF_PASSING,
            ((BigInteger) reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
      if (null != reqMap.get(JsonKey.PERCENTAGE)) {
        reqMap.put(JsonKey.PERCENTAGE,
            Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
      }
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
      try {
        cassandraOperation.insertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), reqMap);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }


  }

  private void updateUserExtId(Map<String, Object> requestMap, DbInfo usrExtIdDb) {
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

      if (!ProjectUtil.isStringNullOREmpty((String) requestMap.get(JsonKey.PHONE_VERIFIED))
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

      if (!ProjectUtil.isStringNullOREmpty((String) requestMap.get(JsonKey.EMAIL_VERIFIED)) &&
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

  private void removeUnwanted(Map<String, Object> reqMap) {
    reqMap.remove(JsonKey.ADDRESS);
    reqMap.remove(JsonKey.EDUCATION);
    reqMap.remove(JsonKey.JOB_PROFILE);
    reqMap.remove(JsonKey.ORGANISATION);
    reqMap.remove(JsonKey.EMAIL_VERIFIED);
    reqMap.remove(JsonKey.PHONE_NUMBER_VERIFIED);
    reqMap.remove(JsonKey.REGISTERED_ORG);
    reqMap.remove(JsonKey.ROOT_ORG);
    reqMap.remove(JsonKey.IDENTIFIER);
    reqMap.remove(JsonKey.ORGANISATIONS);
    reqMap.remove(JsonKey.IS_DELETED);
    reqMap.remove(JsonKey.PHONE_VERIFIED);
  }

  /**
   * Utility method to provide the unique authtoken .
   */
  @SuppressWarnings("unchecked")
  private void checkForDuplicateUserAuthToken(Map<String, Object> userAuthMap, Map<String, Object>
      resultMap, Map<String, Object> reqMap) {
    Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
    String userAuth = null;
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
    map.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
    Response authResponse = cassandraOperation.getRecordsByProperties(userAuthDbInfo.getKeySpace(),
        userAuthDbInfo.getTableName(), map);
    List<Map<String, Object>> userAuthList = ((List<Map<String, Object>>) authResponse
        .get(JsonKey.RESPONSE));
    if (null != userAuthList && userAuthList.isEmpty()) {
      cassandraOperation
          .insertRecord(userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(), userAuthMap);
    } else {
      cassandraOperation.deleteRecord(userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(),
          (String) (userAuthList.get(0)).get(JsonKey.ID));
      userAuth = ProjectUtil.createUserAuthToken((String) resultMap.get(JsonKey.ID),
          (String) reqMap.get(JsonKey.SOURCE));
      userAuthMap.put(JsonKey.ID, userAuth);
      userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      cassandraOperation
          .insertRecord(userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(), userAuthMap);
    }
  }

  /**
   * This method will provide the complete role structure..
   *
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void getRoles() {
    Util.DbInfo roleDbInfo = Util.dbInfoMap.get(JsonKey.ROLE);
    Util.DbInfo roleGroupDbInfo = Util.dbInfoMap.get(JsonKey.ROLE_GROUP);
    Util.DbInfo urlActionDbInfo = Util.dbInfoMap.get(JsonKey.URL_ACTION);
    Response mergeResponse = new Response();
    List<Map<String, Object>> resposnemap = new ArrayList<>();
    List<Map<String, Object>> list = null;
    Response response = cassandraOperation
        .getAllRecords(roleDbInfo.getKeySpace(), roleDbInfo.getTableName());
    Response rolegroup = cassandraOperation
        .getAllRecords(roleGroupDbInfo.getKeySpace(), roleGroupDbInfo.getTableName());
    Response urlAction = cassandraOperation
        .getAllRecords(urlActionDbInfo.getKeySpace(), urlActionDbInfo.getTableName());
    List<Map<String, Object>> urlActionListMap = (List<Map<String, Object>>) urlAction.getResult()
        .get(JsonKey.RESPONSE);
    List<Map<String, Object>> roleGroupMap = (List<Map<String, Object>>) rolegroup.getResult()
        .get(JsonKey.RESPONSE);
    list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    if (list != null && !(list.isEmpty())) {
      //This map will have all the master roles
      for (Map<String, Object> map : list) {
        Map<String, Object> roleResponseMap = new HashMap<>();
        roleResponseMap.put(JsonKey.ID, map.get(JsonKey.ID));
        roleResponseMap.put(JsonKey.NAME, map.get(JsonKey.NAME));
        List<String> roleGroup = (List) map.get(JsonKey.ROLE_GROUP_ID);
        List<Map<String, Object>> actionGroupListMap = new ArrayList<>();
        roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
        Map<String, Object> subRoleResponseMap = null;
        for (String val : roleGroup) {
          subRoleResponseMap = new HashMap<>(); 
          Map<String, Object> subRoleMap = getSubRoleListMap(roleGroupMap, val);
          List<String> subRole = (List) subRoleMap.get(JsonKey.URL_ACTION_ID);
          List<Map<String, Object>> roleUrlResponList = new ArrayList<>();
          subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
          subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));
          for (String rolemap : subRole) {
            roleUrlResponList.add(getRoleAction(urlActionListMap, rolemap));
          }
          if(subRoleResponseMap.containsKey(JsonKey.ACTIONS)) {
            List<Map<String, Object>>  listOfMap  = (List<Map<String, Object>> )subRoleResponseMap.get(JsonKey.ACTIONS);
            listOfMap.addAll(roleUrlResponList);
          } else {
            subRoleResponseMap.put(JsonKey.ACTIONS, roleUrlResponList);
          }
          actionGroupListMap.add(subRoleResponseMap); 
        }
        
        resposnemap.add(roleResponseMap);
      }
    }
    mergeResponse.getResult().put(JsonKey.ROLES, resposnemap);
    sender().tell(mergeResponse, self());
  }


  /**
   * This method will find the action from role action mapping it will return
   * action id, action name and list of urls.
   *
   * @param urlActionListMap List<Map<String,Object>>
   * @param actionName String
   * @return Map<String,Object>
   */
  @SuppressWarnings("rawtypes")
  private Map<String, Object> getRoleAction(List<Map<String, Object>> urlActionListMap,
      String actionName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(actionName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(JsonKey.URLS, (List) (map.get(JsonKey.URL) != null ? map.get(JsonKey.URL)
              : new ArrayList<String>()));
          return response;
        }
      }
    }
    return response;
  }

  /**
   * This method will provide sub role mapping details.
   *
   * @param urlActionListMap List<Map<String, Object>>
   * @param roleName String
   * @return Map< String, Object>
   */
  @SuppressWarnings("rawtypes")
  private Map<String, Object> getSubRoleListMap(List<Map<String, Object>> urlActionListMap,
      String roleName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(roleName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(JsonKey.URL_ACTION_ID,
              (List) (map.get(JsonKey.URL_ACTION_ID) != null ? map.get(JsonKey.URL_ACTION_ID)
                  : new ArrayList<>()));
          return response;
        }
      }
    }
    return response;
  }

  /**
   * Method to join the user with organisation ...
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void joinUserOrganisation(Request actorMessage) {

    Response response = null;  

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    String updatedBy = null;
    String orgId = null;
    String userId = null;
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(req.get(JsonKey.REQUESTED_BY))) {
      updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
    }

    if (isNull(orgId) || isNull(userId)) {
      //create exception here invalid request data and tell the exception , then return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    //check org exist or not
    Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
        organisationDbInfo.getTableName(), orgId);

    List orgList = (List) orgResult.get(JsonKey.RESPONSE);
    if (orgList.isEmpty()) {
      // user already enrolled for the organisation
      ProjectLogger.log("Org does not exist");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation
        .getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(),
            requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      // user already enrolled for the organisation
      response = new Response();
      response.getResult().put(JsonKey.RESPONSE, "User already joined the organisation");
      sender().tell(response, self());
      return;
    }

    String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    usrOrgData.put(JsonKey.ID, id);
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      usrOrgData.put(JsonKey.ADDED_BY_NAME, updatedByName);
      usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
    }
    usrOrgData.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.IS_REJECTED, false);
    usrOrgData.put(JsonKey.IS_APPROVED, false);

    response = cassandraOperation
        .insertRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), usrOrgData);
    sender().tell(response, self());
    return;
  }

  /**
   * Method to approve the user organisation .
   */
  @SuppressWarnings("unchecked")
  private void approveUserOrg(Request actorMessage) {

    Response response = null;
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    String orgId = null;
    String userId = null;
    List<String> roles = null;
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(usrOrgData.get(JsonKey.ROLES))) {
      roles = (List<String>) usrOrgData.get(JsonKey.ROLES);
    }

    if (isNull(orgId) || isNull(userId) || isNull(roles)) {
      //create exception here invalid request data and tell the exception , then return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation
        .getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(),
            requestData);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.isEmpty()) {
      // user already enrolled for the organisation
      ProjectLogger.log("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> userOrgDBO = list.get(0);

    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
    }
    updateUserOrgDBO.put(JsonKey.ID, (String) userOrgDBO.get(JsonKey.ID));
    updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

    updateUserOrgDBO.put(JsonKey.IS_APPROVED, true);
    updateUserOrgDBO.put(JsonKey.IS_REJECTED, false);
    updateUserOrgDBO.put(JsonKey.ROLES, roles);

    response = cassandraOperation
        .updateRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    
    return;

  }

  /**
   * Method to reject the user organisation .
   */
  private void rejectUserOrg(Request actorMessage) {

    Response response = null;
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    @SuppressWarnings("unchecked")
    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    String orgId = null;
    String userId = null;

    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }

    if (isNull(orgId) || isNull(userId)) {
      //creating exception here, invalid request data and tell the exception , then return
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation
        .getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(),
            requestData);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.isEmpty()) {
      // user already enrolled for the organisation
      ProjectLogger.log("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> userOrgDBO = list.get(0);

    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
    }
    updateUserOrgDBO.put(JsonKey.ID, (String) userOrgDBO.get(JsonKey.ID));
    updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

    updateUserOrgDBO.put(JsonKey.IS_APPROVED, false);
    updateUserOrgDBO.put(JsonKey.IS_REJECTED, true);

    response = cassandraOperation
        .updateRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    return;

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
   * @param actorMessage
   */
  private Map<String, Object> getUserDetails(Request actorMessage) {
    Map<String, Object> requestMap = actorMessage.getRequest();
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<String, Object>();
    map.put(JsonKey.REGISTERED_ORG_ID, "some value");
    map.put(JsonKey.ROOT_ORG_ID, "");
    Map<String, Object> additionalProperty = new HashMap<>();
    additionalProperty.put(JsonKey.FILTERS, map);
    dto.setAdditionalProperties(additionalProperty);
    Map<String, Object> responseMap = ElasticSearchUtil
        .complexSearch(dto, ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName());
    if (requestMap != null) {
      return responseMap;
    }
    return null;
  }

  /**
   * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void blockUser(Request actorMessage) {

    ProjectLogger.log("Method call  "+"deleteUser");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if(ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;

    }
    String userId = (String)userMap.get(JsonKey.USER_ID);
    Response resultFrUserId = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),
        userId);
    if(((List<Map<String,Object>>)resultFrUserId.get(JsonKey.RESPONSE)).isEmpty()) {
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String , Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED , true);
    dbMap.put(JsonKey.STATUS , Status.INACTIVE.getValue());
    dbMap.put(JsonKey.ID , userId);
    dbMap.put(JsonKey.USER_ID , userId);
    dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    dbMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));

    // deactivate from keycloak -- softdelete
    boolean isSSOEnabled = Boolean
        .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
    if (isSSOEnabled) {
      SSOManager ssoManager = new KeyCloakServiceImpl();
      ssoManager.deactivateUser(dbMap);
    }
    //soft delete from cassandra--
    Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),dbMap);
    ProjectLogger.log("USER DELETED "+userId);
    sender().tell(response,self());

    // update record in elasticsearch ......
    Response usrResponse = new Response();
    usrResponse.getResult()
        .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    usrResponse.getResult().put(JsonKey.ID, userId);
    backGroundActorRef.tell(usrResponse,self());
  }
  
  
  /**
   * This method will assign roles to users or user organizations.
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void assignRoles(Request actorMessage) {
     Map<String,Object> requestMap  =  actorMessage.getRequest();
     if(requestMap == null || requestMap.size()==0) {
       ProjectCommonException exception = new ProjectCommonException(
           ResponseCode.invalidRequestData.getErrorCode(),
           ResponseCode.invalidRequestData.getErrorMessage(),
           ResponseCode.CLIENT_ERROR.getResponseCode());
       sender().tell(exception, self());
       return;
     }
     Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
      String userId = (String)requestMap.get(JsonKey.USER_ID) ;
      Map<String,Object> tempMap = new HashMap<>();
      String externalId = (String) requestMap.get(JsonKey.EXTERNAL_ID);
      String provider = (String) requestMap.get(JsonKey.PROVIDER);
      String userName = (String) requestMap.get(JsonKey.USERNAME);
       if (ProjectUtil.isStringNullOREmpty(userId)) {
           if(ProjectUtil.isStringNullOREmpty(userName) || ProjectUtil.isStringNullOREmpty(provider)) {
             ProjectCommonException exception = new ProjectCommonException(
                 ResponseCode.invalidUserId.getErrorCode(),
                 ResponseCode.invalidUserId.getErrorMessage(),
                 ResponseCode.CLIENT_ERROR.getResponseCode());
             sender().tell(exception, self());
             return; 
           }
           tempMap.put(JsonKey.LOGIN_ID, userName+JsonKey.LOGIN_ID_DELIMETER+provider);
          Response response = cassandraOperation.getRecordsByProperties(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), tempMap);
          List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
          if (list.isEmpty()) {
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidUsrData.getErrorCode(), ResponseCode.invalidUsrData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
          requestMap.put(JsonKey.USER_ID, list.get(0).get(JsonKey.ID));
       }
     String orgId = (String) requestMap.get(JsonKey.ORGANISATION_ID);
     if(ProjectUtil.isStringNullOREmpty(orgId)) {
        if(!ProjectUtil.isStringNullOREmpty(externalId) && !ProjectUtil.isStringNullOREmpty(provider)) {
          tempMap.remove(JsonKey.LOGIN_ID);
          tempMap.put(JsonKey.EXTERNAL_ID, externalId);
          tempMap.put(JsonKey.PROVIDER, provider);
          Util.DbInfo orgDBInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
          Response result = cassandraOperation.getRecordsByProperties(orgDBInfo.getKeySpace(),
              orgDBInfo.getTableName(), tempMap);
          List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

          if (list.isEmpty()) {
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidOrgData.getErrorCode(), ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return ;
          }
          requestMap.put(JsonKey.ORGANISATION_ID, list.get(0).get(JsonKey.ID));
        }
     }
     //now we have valid userid , roles and need to check organisation id is also coming.
     //if organisationid is coming it means need to update userOrg role.
     //if organisationId is not coming then need to update only userRole.
     if(requestMap.containsKey(JsonKey.ORGANISATION_ID)) {
       tempMap.remove(JsonKey.EXTERNAL_ID);
       tempMap.remove(JsonKey.SOURCE);
       tempMap.put(JsonKey.ORGANISATION_ID, requestMap.get(JsonKey.ORGANISATION_ID));
       tempMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
       Util.DbInfo userOrgDb = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
       Response response = cassandraOperation.getRecordsByProperties(userOrgDb.getKeySpace(), userOrgDb.getTableName(), tempMap);
       List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
       if (list.isEmpty()) {
         ProjectCommonException exception = new ProjectCommonException(
             ResponseCode.invalidUsrData.getErrorCode(), ResponseCode.invalidUsrData.getErrorMessage(),
             ResponseCode.CLIENT_ERROR.getResponseCode());
         sender().tell(exception, self());
         return;
       }
       tempMap.put(JsonKey.ID, list.get(0).get(JsonKey.ID));
       tempMap.put(JsonKey.ROLES, requestMap.get(JsonKey.ROLES));
       response = cassandraOperation.updateRecord(userOrgDb.getKeySpace(), userOrgDb.getTableName(), tempMap);
       sender().tell(response,self());
       if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
         updateRoleToEs(tempMap,JsonKey.ORGANISATION,(String)requestMap.get(JsonKey.USER_ID),(String)requestMap.get(JsonKey.ORGANISATION_ID));
       } else {
         ProjectLogger.log("no call for ES to save user");
       }
       return;
       
     }else {
       tempMap.remove(JsonKey.EXTERNAL_ID);
       tempMap.remove(JsonKey.SOURCE);
       tempMap.remove(JsonKey.ORGANISATION_ID);
       tempMap.put(JsonKey.ID, requestMap.get(JsonKey.USER_ID));
       tempMap.put(JsonKey.ROLES, requestMap.get(JsonKey.ROLES));
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), tempMap);
       ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), (String)requestMap.get(JsonKey.USER_ID), tempMap);
       sender().tell(response,self());
       if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
         updateRoleToEs(tempMap,JsonKey.USER,(String)requestMap.get(JsonKey.USER_ID),null);
       } else {
         ProjectLogger.log("no call for ES to save user");
       }
       return;
     }
  }

  private void updateRoleToEs(Map<String, Object> tempMap, String type,String userid,String orgId) {
    
      ProjectLogger.log("method call going to satrt for ES--.....");
      Response usrResponse = new Response();
      usrResponse.getResult()
          .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_ROLES_ES.getValue());
      usrResponse.getResult().put(JsonKey.ROLES, tempMap.get(JsonKey.ROLES));
      usrResponse.getResult().put(JsonKey.TYPE,type);
      usrResponse.getResult().put(JsonKey.USER_ID,userid);
      usrResponse.getResult().put(JsonKey.ORGANISATION_ID,orgId);
      ProjectLogger.log("making a call to save user data to ES");
      try {
        backGroundActorRef.tell(usrResponse,self());
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occured during saving user to Es while joinUserOrganisation : ", ex);
      }
    
    return;
  }
    
  

  /**
   * Method to un block the user 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void unBlockUser(Request actorMessage) {

    ProjectLogger.log("Method call  "+"UnblockeUser");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if(ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String userId = (String)userMap.get(JsonKey.USER_ID);
    Response resultFrUserId = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),
        userId);
    List<Map<String,Object>> dbResult = (List<Map<String,Object>>)resultFrUserId.get(JsonKey.RESPONSE);
    if(dbResult.isEmpty()) {
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String,Object> dbUser = dbResult.get(0);
    if(dbUser.containsKey(JsonKey.IS_DELETED) && isNotNull(dbUser.get(JsonKey.IS_DELETED)) && !((Boolean)dbUser.get(JsonKey.IS_DELETED))){
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.userAlreadyActive.getErrorCode(),
          ResponseCode.userAlreadyActive.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String , Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED , false);
    dbMap.put(JsonKey.STATUS , Status.ACTIVE.getValue());
    dbMap.put(JsonKey.ID , userId);
    dbMap.put(JsonKey.USER_ID , userId);
    dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    dbMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));

    // Activate user from keycloak
    boolean isSSOEnabled = Boolean
        .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
    if (isSSOEnabled) {
      SSOManager ssoManager = new KeyCloakServiceImpl();
      ssoManager.activateUser(dbMap);
    }
    //Activate user from cassandra-
    Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),dbMap);
    ProjectLogger.log("USER UNLOCKED "+userId);
    sender().tell(response,self());

    // make user active in elasticsearch ......
    Response usrResponse = new Response();
    usrResponse.getResult()
        .put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    usrResponse.getResult().put(JsonKey.ID, userId);
    backGroundActorRef.tell(usrResponse,self());
  }
 
  /**
   * This method will remove user private field from response map
   * @param responseMap Map<String,Object>
   */
  private Map<String,Object> removeUserPrivateField (Map<String,Object> responseMap) {
    ProjectLogger.log("Start removing User private field==");
      for (int i=0;i<ProjectUtil.excludes.length;i++) {
            responseMap.remove(ProjectUtil.excludes[i]);
      }
      ProjectLogger.log("All private filed removed=");  
    return responseMap;
  }
  
  private Map<String , Object> elasticSearchComplexSearch(Map<String , Object> filters , String index , String type) {

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS , filters);

    return ElasticSearchUtil.complexSearch(searchDTO , index,type);

  }
  
}