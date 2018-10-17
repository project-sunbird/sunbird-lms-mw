package org.sunbird.user.actors;

import akka.actor.ActorRef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.dao.UserDao;
import org.sunbird.user.dao.impl.UserDaoImpl;

/**
 * This actor will handle user status operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 * @author sudhirgiri
 */
@ActorConfig(
  tasks = {"unblockUser", "blockUser"},
  asyncTasks = {}
)
public class UserStatusActor extends BaseActor {
  private UserDao userDao = UserDaoImpl.getInstance();
  private SSOManager ssoManager = SSOServiceFactory.getInstance();
  private boolean isSSOEnabled =
      Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
  private ActorRef systemSettingActorRef =
      getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());

  /** Receives the actor message and perform the user status operation . */
  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    switch (operation) {
      case "blockUser":
        blockUser(request);
        break;

      case "unblockUser":
        unblockUser(request);
        break;

      default:
        ProjectLogger.log("UNSUPPORTED OPERATION");
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
    }
  }

  /**
   * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
   *
   * @param actorMessage
   */
  private void blockUser(Request actorMessage) {

    ProjectLogger.log("Method call  " + "deleteUser");
    // Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
    User user = userDao.getUserById(userId);
    if (null == user) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
              ResponseCode.userNotFound.getErrorMessage(),
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> dbMap =
        createUserMap(userId, (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY), true);
    // deactivate from keycloak -- softdelete
    if (isSSOEnabled) {
      ssoManager.deactivateUser(dbMap);
    }
    // soft delete from cassandra--
    Response response = userDao.updateUser(user);
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
  private void unblockUser(Request actorMessage) {

    ProjectLogger.log("Method call  " + "UnblockeUser");
    Util.getUserProfileConfig(systemSettingActorRef);
    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
    User user = userDao.getUserById(userId);
    if (null == user) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
              ResponseCode.userNotFound.getErrorMessage(),
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    if (!user.getIsDeleted()) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userAlreadyActive.getErrorCode(),
              ResponseCode.userAlreadyActive.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String, Object> dbMap =
        createUserMap(userId, (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY), false);
    // Activate user from keycloak
    if (isSSOEnabled) {
      ssoManager.activateUser(dbMap);
    }
    // Activate user from cassandra-
    Response response = userDao.updateUser(user);
    ProjectLogger.log("USER UNLOCKED " + userId);
    sender().tell(response, self());
    // update record in elasticsearch ......
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("UserManagementActor:unblockUser : updating user data to ES.");
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userId);
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log(
            "UserManagementActor:unblockUser : Exception occurred while unblocking user : ", ex);
      }
    } else {
      ProjectLogger.log("UserManagementActor:unblockUser : no call for ES to save user");
    }
    generateTeleEventForUser(null, userId, "unblockUser");
  }

  private Map<String, Object> createUserMap(String userId, String updatedBy, boolean isDeleted) {
    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED, isDeleted);
    if (isDeleted) dbMap.put(JsonKey.STATUS, Status.INACTIVE.getValue());
    else dbMap.put(JsonKey.STATUS, Status.ACTIVE.getValue());
    dbMap.put(JsonKey.ID, userId);
    dbMap.put(JsonKey.USER_ID, userId);
    dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    dbMap.put(JsonKey.UPDATED_BY, updatedBy);

    return dbMap;
  }

  private void generateTeleEventForUser(
      Map<String, Object> requestMap, String userId, String objectType) {
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, Object> targetObject =
        TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
    Map<String, Object> telemetryAction = new HashMap<>();
    if (objectType.equalsIgnoreCase("orgLevel")) {
      telemetryAction.put("AssignRole", "role assigned at org level");
      if (null != requestMap) {
        TelemetryUtil.generateCorrelatedObject(
            (String) requestMap.get(JsonKey.ORGANISATION_ID),
            JsonKey.ORGANISATION,
            null,
            correlatedObject);
      }
    } else {
      if (objectType.equalsIgnoreCase("userLevel")) {
        telemetryAction.put("AssignRole", "role assigned at user level");
      } else if (objectType.equalsIgnoreCase("blockUser")) {
        telemetryAction.put("BlockUser", "user blocked");
      } else if (objectType.equalsIgnoreCase("unBlockUser")) {
        telemetryAction.put("UnBlockUser", "user unblocked");
      } else if (objectType.equalsIgnoreCase("profileVisibility")) {
        telemetryAction.put("ProfileVisibility", "profile Visibility setting changed");
      }
    }
    TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
  }
}
