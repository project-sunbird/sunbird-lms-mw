package org.sunbird.user.actors;

import static akka.pattern.PatternsCS.ask;

import akka.util.Timeout;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.util.UserActorOperations;
import scala.concurrent.duration.Duration;

@ActorConfig(
  tasks = {},
  asyncTasks = {"saveUserAttributes"}
)
public class UserAttributesProcessingActor extends BaseActor {

  private static InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();
  private static Timeout t = new Timeout(Duration.create(10, TimeUnit.SECONDS));

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.SAVE_USER_ATTRIBUTES
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
    ProjectLogger.log("saveUserAttributes called");
    Response response = null;
    Map<String, Object> userMap = request.getRequest();
    String operationType = (String) userMap.get(JsonKey.OPERATION_TYPE);
    userMap.remove(JsonKey.OPERATION_TYPE);
    response = saveAddress(userMap, operationType);
    if (!JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
      userMap.remove(JsonKey.ADDRESS);
      response = null;
    }
    response = saveEducation(userMap, operationType);
    if (!JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
      userMap.remove(JsonKey.ADDRESS);
      response = null;
    }
    response = saveJobProfile(userMap, operationType);
    if (!JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
      userMap.remove(JsonKey.ADDRESS);
      response = null;
    }
    response = saveUserExternalIds(userMap, operationType);
    if (!JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
      userMap.remove(JsonKey.ADDRESS);
      response = null;
    }
    response = saveUserOrgDetails(userMap, operationType);
    if (!JsonKey.SUCCESS.equalsIgnoreCase((String) response.get(JsonKey.RESPONSE))) {
      userMap.remove(JsonKey.ADDRESS);
      response = null;
    }
    Request userRequest = new Request();
    userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    userRequest.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
    tellToAnother(request);
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
        CompletableFuture<Object> future =
            ask(
                    BackgroundRequestRouter.getActor(
                        UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue()),
                    userExternalIdsRequest,
                    t)
                .toCompletableFuture();
        return (Response) future.get(12, TimeUnit.SECONDS);
        // return (Response)
        // interServiceCommunication.getResponse(BackgroundRequestRouter.getActor(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue()), userExternalIdsRequest);

        // tellToAnother(userExternalIdsRequest);
        // ProjectLogger.log("tellToAnother(userExternalIdsRequest)");
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
        CompletableFuture<Object> future =
            ask(
                    BackgroundRequestRouter.getActor(
                        UserActorOperations.UPSERT_USER_ORG_DETAILS.getValue()),
                    userOrgRequest,
                    t)
                .toCompletableFuture();
        return (Response) future.get(12, TimeUnit.SECONDS);
        // return (Response)
        // interServiceCommunication.getResponse(BackgroundRequestRouter.getActor(UserActorOperations.UPSERT_USER_ORG_DETAILS.getValue()), userOrgRequest);

        // tellToAnother(userOrgRequest);
        // ProjectLogger.log("tellToAnother(userOrgRequest)");
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
        CompletableFuture<Object> future =
            ask(
                    BackgroundRequestRouter.getActor(
                        UserActorOperations.UPSERT_USER_JOB_PROFILE.getValue()),
                    jobProfileRequest,
                    t)
                .toCompletableFuture();
        return (Response) future.get(12, TimeUnit.SECONDS);
        // return (Response)
        // interServiceCommunication.getResponse(BackgroundRequestRouter.getActor(UserActorOperations.UPSERT_USER_JOB_PROFILE.getValue()), jobProfileRequest);

        // tellToAnother(jobProfileRequest);
        // ProjectLogger.log("tellToAnother(jobProfileRequest)");
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
        CompletableFuture<Object> future =
            ask(
                    BackgroundRequestRouter.getActor(
                        UserActorOperations.UPSERT_USER_EDUCATION.getValue()),
                    educationRequest,
                    t)
                .toCompletableFuture();
        return (Response) future.get(12, TimeUnit.SECONDS);
        // return (Response)
        // interServiceCommunication.getResponse(BackgroundRequestRouter.getActor(UserActorOperations.UPSERT_USER_EDUCATION.getValue()), educationRequest);

        // tellToAnother(educationRequest);
        // ProjectLogger.log("tellToAnother(educationRequest)");
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
        CompletableFuture<Object> future =
            ask(
                    BackgroundRequestRouter.getActor(
                        UserActorOperations.UPSERT_USER_ADDRESS.getValue()),
                    addressRequest,
                    t)
                .toCompletableFuture();
        return (Response) future.get(12, TimeUnit.SECONDS);
        // return (Response)
        // interServiceCommunication.getResponse(BackgroundRequestRouter.getActor(UserActorOperations.UPSERT_USER_ADDRESS.getValue()), addressRequest);
        // tellToAnother(addressRequest);
        // ProjectLogger.log("tellToAnother(saveAddress)");
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserAttributesProcessingActor:saveAddress: Exception occurred while saving user address details.",
          ex);
    }
    return null;
  }
}
