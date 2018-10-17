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

/**
 * This actor will handle user login operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 * @author sudhirgiri
 */
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

    switch (operation) {
      case "userCurrentLogin":
        updateUserLoginTime(request);
        break;

      default:
        onReceiveUnsupportedOperation("UserLoginActor");
    }
  }

  /**
   * This method will update user current login time in keycloak
   *
   * @param actorMessage Request
   */
  private void updateUserLoginTime(Request actorMessage) {
    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
    if (Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
      boolean addedResponse = ssoManager.addUserLoginTime(userId);
      ProjectLogger.log("user login time added response is ==" + addedResponse);
    }
  }
}
