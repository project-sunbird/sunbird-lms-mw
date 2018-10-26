package org.sunbird.user.actors;

import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {"saveUserAttributes"},
  asyncTasks = {"saveUserAttributes"}
)
public class UserAttributesProcessingActor extends BaseActor {

  private static InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.SAVE_USER_ATTRIBUTES
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      saveUserAttributes(request);
    } else {
      onReceiveUnsupportedOperation("UserAttributesProcessingActor");
    }
  }

  private void saveUserAttributes(Request request) {
    Response response = new Response();
    Map<String, Object> userMap = request.getRequest();
    String operationType = (String) userMap.get(JsonKey.OPERATION_TYPE);
    userMap.remove(JsonKey.OPERATION_TYPE);
    response.put(JsonKey.ADDRESS, saveAddress(userMap, operationType));
    response.put(JsonKey.EDUCATION, saveEducation(userMap, operationType));
    response.put(JsonKey.JOB_PROFILE, saveJobProfile(userMap, operationType));
    response.put(JsonKey.EXTERNAL_IDS, saveUserExternalIds(userMap, operationType));
    response.put(JsonKey.USER_ORG, saveUserOrgDetails(userMap, operationType));
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private Response saveUserExternalIds(Map<String, Object> userMap, String operationType) {
    try {
      if (CollectionUtils.isNotEmpty(
          (List<Map<String, String>>) userMap.get(JsonKey.EXTERNAL_IDS))) {
        Request userExternalIdsRequest = new Request();
        userExternalIdsRequest.getRequest().putAll(userMap);
        userExternalIdsRequest.getRequest().put(JsonKey.OPERATION_TYPE, operationType);
        userExternalIdsRequest.setOperation(
            UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());
        return (Response)
            interServiceCommunication.getResponse(
                getActorRef(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue()),
                userExternalIdsRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveUserExternalIds: Exception occurred while saving user externalIds.",
          ex);
    }
    return null;
  }

  private Response saveUserOrgDetails(Map<String, Object> userMap, String operationType) {
    try {
      if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORGANISATION_ID))
          || StringUtils.isNotBlank((String) userMap.get(JsonKey.ROOT_ORG_ID))) {
        Request userOrgRequest = new Request();
        userOrgRequest.getRequest().putAll(userMap);
        userOrgRequest.getRequest().put(JsonKey.OPERATION_TYPE, operationType);
        userOrgRequest.setOperation(UserActorOperations.UPSERT_USER_ORG_DETAILS.getValue());
        return (Response)
            interServiceCommunication.getResponse(
                getActorRef(UserActorOperations.UPSERT_USER_ORG_DETAILS.getValue()),
                userOrgRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveUserOrgDetails: Exception occurred while saving user org details.",
          ex);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Response saveJobProfile(Map<String, Object> userMap, String operationType) {
    try {
      if (userMap.containsKey(JsonKey.JOB_PROFILE)
          && CollectionUtils.isNotEmpty(
              (List<Map<String, Object>>) userMap.get(JsonKey.JOB_PROFILE))) {
        Request jobProfileRequest = new Request();
        jobProfileRequest.getRequest().putAll(userMap);
        jobProfileRequest.getRequest().put(JsonKey.OPERATION_TYPE, operationType);
        jobProfileRequest.setOperation(UserActorOperations.UPSERT_USER_JOB_PROFILE.getValue());
        return (Response)
            interServiceCommunication.getResponse(
                getActorRef(UserActorOperations.UPSERT_USER_JOB_PROFILE.getValue()),
                jobProfileRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveJobProfile: Exception occurred while saving user job profile details.",
          ex);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Response saveEducation(Map<String, Object> userMap, String operationType) {
    try {
      if (userMap.containsKey(JsonKey.EDUCATION)
          && CollectionUtils.isNotEmpty(
              (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION))) {
        Request educationRequest = new Request();
        educationRequest.getRequest().putAll(userMap);
        educationRequest.getRequest().put(JsonKey.OPERATION_TYPE, operationType);
        educationRequest.setOperation(UserActorOperations.UPSERT_USER_EDUCATION.getValue());
        return (Response)
            interServiceCommunication.getResponse(
                getActorRef(UserActorOperations.UPSERT_USER_EDUCATION.getValue()),
                educationRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveEducation: Exception occurred while saving user education details.",
          ex);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Response saveAddress(Map<String, Object> userMap, String operationType) {
    try {
      if (userMap.containsKey(JsonKey.ADDRESS)
          && CollectionUtils.isNotEmpty((List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS))) {
        Request addressRequest = new Request();
        addressRequest.getRequest().putAll(userMap);
        addressRequest.getRequest().put(JsonKey.OPERATION_TYPE, operationType);
        addressRequest.setOperation(UserActorOperations.UPSERT_USER_ADDRESS.getValue());
        return (Response)
            interServiceCommunication.getResponse(
                getActorRef(UserActorOperations.UPSERT_USER_ADDRESS.getValue()), addressRequest);
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveAddress: Exception occurred while saving user address details.",
          ex);
    }
    return null;
  }
}
