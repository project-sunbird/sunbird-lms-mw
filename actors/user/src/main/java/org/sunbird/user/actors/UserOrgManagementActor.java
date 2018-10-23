package org.sunbird.user.actors;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {},
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
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void upsertUserOrgDetails(Request request) {
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
    // saveUserOrgDetailsToEs(requestMap);
  }

  private void saveUserOrgDetailsToEs(Map<String, Object> requestMap) {
    Request userRequest = new Request();
    userRequest.setOperation(UserActorOperations.UPSERT_USER_ORG_DETAILS_TO_ES.getValue());
    userRequest.getRequest().putAll(requestMap);
    tellToAnother(userRequest);
  }
}
