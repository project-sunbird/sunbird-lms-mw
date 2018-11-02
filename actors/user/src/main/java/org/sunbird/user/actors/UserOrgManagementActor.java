package org.sunbird.user.actors;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

@ActorConfig(
  tasks = {"insertUserOrgDetails", "updateUserOrgDetails"},
  asyncTasks = {"insertUserOrgDetails", "updateUserOrgDetails"}
)
public class UserOrgManagementActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "insertUserOrgDetails":
        insertUserOrgDetails(request);
        break;

      default:
        onReceiveUnsupportedOperation("UserOrgManagementActor");
    }
  }

  private void insertUserOrgDetails(Request request) {
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
