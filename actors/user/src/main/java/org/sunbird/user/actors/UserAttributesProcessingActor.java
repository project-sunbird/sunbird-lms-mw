package org.sunbird.user.actors;

import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {},
  asyncTasks = {"saveUserAttributes"}
)
public class UserAttributesProcessingActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_JOB_PROFILE
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      saveUserAttributes(request);
    } else {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void saveUserAttributes(Request request) {
    Map<String, Object> userMap = request.getRequest();
    saveAddress(userMap);
    saveEducation(userMap);
    saveJobProfile(userMap);
    saveUserOrgDetails(userMap);
    saveUserExternalIds(userMap);
  }

  @SuppressWarnings("unchecked")
  private void saveUserExternalIds(Map<String, Object> userMap) {
    try {
      if (CollectionUtils.isNotEmpty(
          (List<Map<String, String>>) userMap.get(JsonKey.EXTERNAL_IDS))) {
        Request userExternalIdsRequest = new Request();
        userExternalIdsRequest.getRequest().putAll(userMap);
        userExternalIdsRequest.getRequest().put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
        userExternalIdsRequest.setOperation(
            UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());
        tellToAnother(userExternalIdsRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveUserExternalIds: Exception occurred while saving user externalIds.",
          ex);
    }
  }

  private void saveUserOrgDetails(Map<String, Object> userMap) {
    try {
      if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORGANISATION_ID))
          || StringUtils.isNotBlank((String) userMap.get(JsonKey.ROOT_ORG_ID))) {
        Request userOrgRequest = new Request();
        userOrgRequest.getRequest().putAll(userMap);
        userOrgRequest.setOperation(UserActorOperations.UPSERT_USER_ORG_DETAILS.getValue());
        tellToAnother(userOrgRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveUserOrgDetails: Exception occurred while saving user org details.",
          ex);
    }
  }

  private void saveJobProfile(Map<String, Object> userMap) {
    try {
      if (userMap.containsKey(JsonKey.JOB_PROFILE)
          && CollectionUtils.isNotEmpty(
              (List<Map<String, Object>>) userMap.get(JsonKey.JOB_PROFILE))) {
        Request jobProfileRequest = new Request();
        jobProfileRequest.getRequest().putAll(userMap);
        jobProfileRequest.getRequest().put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
        jobProfileRequest.setOperation(UserActorOperations.UPSERT_USER_JOB_PROFILE.getValue());
        tellToAnother(jobProfileRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveJobProfile: Exception occurred while saving user job profile details.",
          ex);
    }
  }

  private void saveEducation(Map<String, Object> userMap) {
    try {
      if (userMap.containsKey(JsonKey.EDUCATION)
          && CollectionUtils.isNotEmpty(
              (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION))) {
        Request educationRequest = new Request();
        educationRequest.getRequest().putAll(userMap);
        educationRequest.getRequest().put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
        educationRequest.setOperation(UserActorOperations.UPSERT_USER_EDUCATION.getValue());
        tellToAnother(educationRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveEducation: Exception occurred while saving user education details.",
          ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void saveAddress(Map<String, Object> userMap) {
    try {
      if (userMap.containsKey(JsonKey.ADDRESS)
          && CollectionUtils.isNotEmpty((List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS))) {
        Request addressRequest = new Request();
        addressRequest.getRequest().putAll(userMap);
        addressRequest.getRequest().put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
        addressRequest.setOperation(UserActorOperations.UPSERT_USER_ADDRESS.getValue());
        tellToAnother(addressRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveAddress: Exception occurred while saving user address details.",
          ex);
    }
  }
}
