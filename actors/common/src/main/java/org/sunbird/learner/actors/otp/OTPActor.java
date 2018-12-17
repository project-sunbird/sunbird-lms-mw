package org.sunbird.learner.actors.otp;

import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.otp.service.OTPService;
import org.sunbird.learner.actors.user.service.UserService;
import org.sunbird.learner.util.OTPUtil;

@ActorConfig(
  tasks = {"generateOTP"},
  asyncTasks = {}
)
public class OTPActor extends BaseActor {

  private UserService userService = new UserService();
  private OTPService otpService = new OTPService();

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
      userService.checkEmailUniqueness(key);
    } else if (JsonKey.PHONE.equalsIgnoreCase(type)) {
      userService.checkPhoneUniqueness(key);
    }

    String otp = null;
    Map<String, Object> details = otpService.getOTPDetailsByKey(type, key);
    if (MapUtils.isEmpty(details)) {
      otp = OTPUtil.generateOTP();
      ProjectLogger.log("OTPActor:generateOTP: OTP = " + otp, LoggerEnum.INFO);
      otpService.insertOTPDetails(type, key, otp);
    } else {
      otp = (String) details.get(JsonKey.OTP);
    }

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());

    sendOTP(request, otp);
  }

  private void sendOTP(Request request, String otp) {
    Request sendOtpRequest = new Request();

    sendOtpRequest.getRequest().putAll(request.getRequest());
    sendOtpRequest.getRequest().put(JsonKey.OTP, otp);
    sendOtpRequest.setOperation(ActorOperations.SEND_OTP.getValue());

    // Sent OTP via email or sms
    tellToAnother(sendOtpRequest);
  }

}
