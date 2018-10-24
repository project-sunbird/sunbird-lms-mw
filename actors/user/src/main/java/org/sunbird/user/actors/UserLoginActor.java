package org.sunbird.user.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

@ActorConfig(
  tasks = {"userCurrentLogin"},
  asyncTasks = {}
)
public class UserLoginActor extends BaseActor {

  private SSOManager ssoManager = SSOServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    if (operation.equalsIgnoreCase("userCurrentLogin")) {
      updateUserLoginTime(request);
    } else {
      onReceiveUnsupportedOperation("UserLoginActor");
    }
  }

  /**
   * Updates user's current login time in Keycloak.
   *
   * @param actorMessage Request containing user ID.
   */
  private void updateUserLoginTime(Request actorMessage) {
    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
    if (Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
      boolean loginTimeResponse = ssoManager.addUserLoginTime(userId);
      ProjectLogger.log(
          "UserLoginActor:updateUserLoginTime: keycloak response = " + loginTimeResponse);
    }
  }
}
