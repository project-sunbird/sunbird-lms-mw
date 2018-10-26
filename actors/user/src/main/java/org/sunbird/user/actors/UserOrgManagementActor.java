package org.sunbird.user.actors;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {"upsertUserOrgDetails"},
  asyncTasks = {"upsertUserOrgDetails"}
)
public class UserOrgManagementActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_ORG_DETAILS
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      upsertUserOrgDetails(request);
    } else {
      onReceiveUnsupportedOperation("UserOrgManagementActor");
    }
  }

  private void upsertUserOrgDetails(Request request) {
    ProjectLogger.log("upsertUserOrgDetails called");
    Map<String, Object> requestMap = request.getRequest();
    // Register user to given orgId(not root orgId)
    String organisationId = (String) requestMap.get(JsonKey.ORGANISATION_ID);
    if (StringUtils.isNotBlank(organisationId)) {
      String hashTagId =
          Util.getHashTagIdFromOrgId((String) requestMap.get(JsonKey.ORGANISATION_ID));
      requestMap.put(JsonKey.HASHTAGID, hashTagId);
      Util.registerUserToOrg(requestMap);
    }
    if ((StringUtils.isNotBlank(organisationId)
            && !organisationId.equalsIgnoreCase((String) requestMap.get(JsonKey.ROOT_ORG_ID)))
        || StringUtils.isBlank(organisationId)) {
      // Add user to root org
      requestMap.put(JsonKey.ORGANISATION_ID, requestMap.get(JsonKey.ROOT_ORG_ID));
      String hashTagId = Util.getHashTagIdFromOrgId((String) requestMap.get(JsonKey.ROOT_ORG_ID));
      requestMap.put(JsonKey.HASHTAGID, hashTagId);
      Util.registerUserToOrg(requestMap);
    }
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
  }
}
