package org.sunbird.learner.actors.data.security.manager;

import java.text.MessageFormat;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

@ActorConfig(
  tasks = {"encryptUserData", "decryptUserData"},
  asyncTasks = {}
)
public class DataSecurityActor extends BaseActor {

  @Override
  public void onReceive(Request actorMessage) throws Throwable {
    String operation = actorMessage.getOperation();
    switch (operation) {
      case "encryptUserData":
        encryptUserData(actorMessage);
        break;
      case "decryptUserData":
        decryptUserData(actorMessage);
        break;
      default:
        onReceiveUnsupportedOperation(actorMessage.getOperation());
        break;
    }
  }

  private void decryptUserData(Request actorMessage) {
    ProjectLogger.log(
        "DecryptUserData API called by " + actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
    encryptionDecryptionData(actorMessage, ActorOperations.BACKGROUND_DECRYPTION.getValue());
  }

  private void encryptUserData(Request actorMessage) {
    ProjectLogger.log(
        "EncryptUserData API called by " + actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
    encryptionDecryptionData(actorMessage, ActorOperations.BACKGROUND_ENCRYPTION.getValue());
  }

  private void encryptionDecryptionData(Request actorMessage, String backgroundOperation) {
    validateUserIdSize(actorMessage);
    long start = System.currentTimeMillis();
    Response resp = new Response();
    resp.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(resp, self());
    Request backgroundEncryptionDecryptionRequest = new Request();
    backgroundEncryptionDecryptionRequest.setOperation(backgroundOperation);
    backgroundEncryptionDecryptionRequest
        .getRequest()
        .put(JsonKey.USER_IDs, actorMessage.getRequest().get(JsonKey.USER_IDs));

    tellToAnother(backgroundEncryptionDecryptionRequest);
    long end = System.currentTimeMillis();
    ProjectLogger.log(
        "UserDataEncryptionDecryptionServiceActor:encryptionDecryptionData: total time taken by "
            + backgroundOperation
            + " user data:::: "
            + (end - start));
  }

  @SuppressWarnings("unchecked")
  private void validateUserIdSize(Request actorMessage) {
    int maximumSizeAllowed =
        Integer.valueOf(
            ProjectUtil.getConfigValue(JsonKey.SUNBIRD_USER_MAX_ENCRYPTION_LIMIT).trim());
    List<String> userIds = (List<String>) actorMessage.getRequest().get(JsonKey.USER_IDs);
    if (CollectionUtils.isNotEmpty(userIds)) {
      if (userIds.size() > maximumSizeAllowed) {
        ProjectCommonException.throwClientErrorException(
            ResponseCode.sizeLimitExceed,
            MessageFormat.format(
                ResponseCode.sizeLimitExceed.getErrorMessage(), maximumSizeAllowed));
      }
    } else {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.mandatoryParamsMissing,
          MessageFormat.format(
              ResponseCode.mandatoryParamsMissing.getErrorMessage(), JsonKey.USER_IDs));
    }
  }
}
