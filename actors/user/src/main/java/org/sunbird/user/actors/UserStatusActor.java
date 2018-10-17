package org.sunbird.user.actors;

import akka.actor.ActorRef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
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
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

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
  private UserService userService = UserServiceImpl.getInstance();
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
        onReceiveUnsupportedOperation("UserStatusActor");
    }
  }

  /**
   * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
   *
   * @param actorMessage
   */
  private void blockUser(Request actorMessage) {
    commonRequestHandler(actorMessage, true);
  }
  /**
   * Method to un block the user
   *
   * @param actorMessage
   */
  private void unblockUser(Request actorMessage) {
    commonRequestHandler(actorMessage, false);
  }

  private void commonRequestHandler(Request request, boolean canBlocked) {
    String operation = request.getOperation();
    ProjectLogger.log("Method call  " + operation);
    Util.getUserProfileConfig(systemSettingActorRef);
    String userId = (String) request.getRequest().get(JsonKey.USER_ID);
    User user = userService.validateUserId(userId);
    if (operation.equals(ActorOperations.BLOCK_USER.getValue()) && user.getIsDeleted()) {
      throw new ProjectCommonException(
          ResponseCode.userAlreadyInactive.getErrorCode(),
          ResponseCode.userAlreadyInactive.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (operation.equals(ActorOperations.UNBLOCK_USER.getValue()) && !user.getIsDeleted()) {
      throw new ProjectCommonException(
          ResponseCode.userAlreadyActive.getErrorCode(),
          ResponseCode.userAlreadyActive.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> userMapES =
        createUserMap(userId, (String) request.getContext().get(JsonKey.REQUESTED_BY), canBlocked);
    if (isSSOEnabled) {
      if (canBlocked) {
        ssoManager.deactivateUser(userMapES);
      } else {
        ssoManager.activateUser(userMapES);
      }
    }
    Response response = userDao.updateUser(user);
    ProjectLogger.log(operation + " completed for " + userId);
    sender().tell(response, self());
    // update record in elasticsearch ......
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("UserManagementActor:" + operation + " : updating user data to ES.");
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userId);
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log("UserStatusActor:" + operation + " : Exception occurred : ", ex);
      }
    } else {
      ProjectLogger.log("UserStatusActor:" + operation + " : no call for ES to save user");
    }
    generateTeleEventForUser(null, userId, operation);
  }

  private Map<String, Object> createUserMap(String userId, String updatedBy, boolean isDeleted) {
    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.IS_DELETED, isDeleted);
    if (isDeleted) {
      dbMap.put(JsonKey.STATUS, Status.INACTIVE.getValue());
    } else {
      dbMap.put(JsonKey.STATUS, Status.ACTIVE.getValue());
    }
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
    if (objectType.equalsIgnoreCase("blockUser")) {
      telemetryAction.put("BlockUser", "user blocked");
    } else {
      telemetryAction.put("UnblockUser", "user unblocked");
    }
    TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
  }
}
