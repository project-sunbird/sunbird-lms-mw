package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;

/** This actor process the request for reset password. */
@ActorConfig(
  tasks = {"resetPassword"},
  asyncTasks = {}
)
public class ResetPasswordActor extends BaseActor {

  private SSOManager ssoManager = SSOServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    // Generate Telemetry (initializing context)
    Util.initializeContext(request, TelemetryEnvKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    String userId = (String) request.get(JsonKey.USER_ID);
    String password = (String) request.get(JsonKey.PASSWORD);
    resetPassword(userId, password);
    generateTelemetry(request.getRequest());
  }

  private void resetPassword(String userId, String password) {
    ProjectLogger.log("ResetPasswordActor:resetPassword: method called.", LoggerEnum.INFO.name());
    if (ssoManager.updatePassword(userId, password)) {
      Response response = new Response();
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      sender().tell(response, self());
    } else {
      ProjectLogger.log(
          "ResetPasswordActor:resetPassword : reset password failed", LoggerEnum.INFO.name());
      ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR);
    }
  }

  private void generateTelemetry(Map<String, Object> request) {
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) request.get(JsonKey.USER_ID), TelemetryEnvKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.generateCorrelatedObject(
        (String) request.get(JsonKey.USER_ID), TelemetryEnvKey.USER, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(request, targetObject, correlatedObject);
  }
}
