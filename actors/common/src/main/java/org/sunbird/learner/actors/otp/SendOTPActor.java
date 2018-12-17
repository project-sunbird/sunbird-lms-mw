package org.sunbird.learner.actors.otp;

import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.OTPUtil;

@ActorConfig(
  tasks = {},
  asyncTasks = {"sendOTP"}
)
public class SendOTPActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {

    if (ActorOperations.SEND_OTP.getValue().equals(request.getOperation())) {
      sendOTP(request);
    } else {
      onReceiveUnsupportedOperation("SendOTPActor");
    }
  }

  private void sendOTP(Request request) {
    String type = (String) request.getRequest().get(JsonKey.TYPE);
    String key = (String) request.getRequest().get(JsonKey.KEY);
    String otp = (String) request.getRequest().get(JsonKey.OTP);

    if (JsonKey.EMAIL.equalsIgnoreCase(type)) {
      sendOTPViaEmail(key, otp);
    } else if (JsonKey.PHONE.equalsIgnoreCase(type)) {
      sendOTPViaSMS(key, otp);
    }
  }

  private void sendOTPViaEmail(String key, String otp) {
    Map<String, Object> emailTemplateMap = new HashMap<>();
    emailTemplateMap.put(JsonKey.EMAIL, key);
    emailTemplateMap.put(JsonKey.OTP, otp);
    emailTemplateMap.put(JsonKey.OTP_EXPIRATION_IN_MINUTES, OTPUtil.getOTPExpirationInMinutes());
    Request emailRequest = OTPUtil.sendOTPMailRequest(emailTemplateMap);
    tellToAnother(emailRequest);
  }

  private void sendOTPViaSMS(String key, String otp) {
    Map<String, Object> otpMap = new HashMap<>();
    otpMap.put(JsonKey.PHONE, key);
    otpMap.put(JsonKey.OTP, otp);
    otpMap.put(JsonKey.OTP_EXPIRATION_IN_MINUTES, OTPUtil.getOTPExpirationInMinutes());
    OTPUtil.sendOTPSMS(otpMap);
  }

}
