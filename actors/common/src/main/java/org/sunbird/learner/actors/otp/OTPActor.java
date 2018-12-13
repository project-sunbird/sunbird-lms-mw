/** */
package org.sunbird.learner.actors.otp;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.OTPUtil;

/** @author Rahul Kumar */
@ActorConfig(
  tasks = {"generateOTP"},
  asyncTasks = {}
)
public class OTPActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (ActorOperations.GENERATE_OTP.getValue().equals(request.getOperation())) {
      generateOTP(request);
    } else {
      onReceiveUnsupportedOperation("OTPActor");
    }
  }

  private void generateOTP(Request request) {
    String type = (String) request.getRequest().get(JsonKey.TYPE);
    String key = (String) request.getRequest().get(JsonKey.KEY);
    if (JsonKey.EMAIL.equalsIgnoreCase(type)) {
      OTPUtil.checkEmailUniqueness(key);
    } else if (JsonKey.PHONE.equalsIgnoreCase(type)) {
      OTPUtil.checkPhoneUniqueness(key);
    }

    String otp = OTPUtil.generateOTP();
    ProjectLogger.log("OTP = " + otp, LoggerEnum.INFO);

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
  }
}
