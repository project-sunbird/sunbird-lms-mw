package org.sunbird.user.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

@ActorConfig(
    tasks = {"unblockUser", "blockUser"},
    asyncTasks = {})
public class UserStatusActor extends UserBaseActor {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    // set request id fto thread loacl...
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();
    if (operation.equalsIgnoreCase(ActorOperations.BLOCK_USER.getValue())) {
      blockUser(request);
    } else if (operation.equalsIgnoreCase(ActorOperations.UNBLOCK_USER.getValue())) {
      unBlockUser(request);
    }
  }

  /**
   * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void blockUser(Request actorMessage) {

    ProjectLogger.log("Method call  " + "deleteUser");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String userId = (String) userMap.get(JsonKey.USER_ID);
    Map<String, Object> userDbRecord = Util.getUserbyUserId(userId);
    if (null == userDbRecord) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
              ResponseCode.userNotFound.getErrorMessage(),
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED, true);
    dbMap.put(JsonKey.STATUS, Status.INACTIVE.getValue());
    dbMap.put(JsonKey.ID, userId);
    dbMap.put(JsonKey.USER_ID, userId);
    dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    dbMap.put(JsonKey.UPDATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));

    // deactivate from keycloak -- softdelete
    if (isSSOEnabled()) {
      getSsoManager().deactivateUser(dbMap);
    }
    // delete from registry
    /*if (IS_REGISTRY_ENABLED) {
      Map<String, Object> regMap = new HashMap<>();
      regMap.put(JsonKey.REGISTRY_ID, userDbRecord.get(JsonKey.REGISTRY_ID));
      UserExtension userExtension = new UserProviderRegistryImpl();
      userExtension.delete(regMap);
    }*/
    // soft delete from cassandra--
    Response response =
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), dbMap);
    ProjectLogger.log("USER DELETED " + userId);
    sender().tell(response, self());

    // update record in elasticsearch ......
    dbMap.remove(JsonKey.ID);
    dbMap.remove(JsonKey.USER_ID);
    ElasticSearchUtil.updateData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        userId,
        dbMap);
    generateTeleEventForUser(null, userId, "blockUser");
  }

  /**
   * Method to un block the user
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void unBlockUser(Request actorMessage) {

    ProjectLogger.log("Method call  " + "UnblockeUser");
    Util.getUserProfileConfig(getSystemSettingActorRef());
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String userId = (String) userMap.get(JsonKey.USER_ID);
    Response resultFrUserId =
        cassandraOperation.getRecordById(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userId);
    List<Map<String, Object>> dbResult =
        (List<Map<String, Object>>) resultFrUserId.get(JsonKey.RESPONSE);
    if (dbResult.isEmpty()) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
              ResponseCode.userNotFound.getErrorMessage(),
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String, Object> dbUser = dbResult.get(0);
    if (dbUser.containsKey(JsonKey.IS_DELETED)
        && isNotNull(dbUser.get(JsonKey.IS_DELETED))
        && !((Boolean) dbUser.get(JsonKey.IS_DELETED))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userAlreadyActive.getErrorCode(),
              ResponseCode.userAlreadyActive.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED, false);
    dbMap.put(JsonKey.STATUS, Status.ACTIVE.getValue());
    dbMap.put(JsonKey.ID, userId);
    dbMap.put(JsonKey.USER_ID, userId);
    dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    dbMap.put(JsonKey.UPDATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));

    // Activate user from keycloak
    if (isSSOEnabled()) {
      getSsoManager().activateUser(dbMap);
    }
    // Activate user from cassandra-
    Response response =
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), dbMap);
    ProjectLogger.log("USER UNLOCKED " + userId);
    sender().tell(response, self());

    // update record in elasticsearch ......
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("UserManagementActor:unBlockUser : updating user data to ES.");
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userId);
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log(
            "UserManagementActor:unBlockUser : Exception occurred while unblocking user : ", ex);
      }
    } else {
      ProjectLogger.log("UserManagementActor:unBlockUser : no call for ES to save user");
    }
    generateTeleEventForUser(null, userId, "unBlockUser");
  }
}
