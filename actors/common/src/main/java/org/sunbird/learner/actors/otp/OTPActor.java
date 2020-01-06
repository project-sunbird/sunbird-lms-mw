package org.sunbird.learner.actors.otp;

import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.otp.service.OTPService;
import org.sunbird.learner.util.OTPUtil;
import org.sunbird.ratelimit.limiter.OtpRateLimiter;
import org.sunbird.ratelimit.limiter.RateLimiter;
import org.sunbird.ratelimit.service.RateLimitService;
import org.sunbird.ratelimit.service.RateLimitServiceImpl;

@ActorConfig(
  tasks = {"generateOTP", "verifyOTP"},
  asyncTasks = {}
)
public class OTPActor extends BaseActor {

  private OTPService otpService = new OTPService();
  private RateLimitService rateLimitService = new RateLimitServiceImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    if (ActorOperations.GENERATE_OTP.getValue().equals(request.getOperation())) {
      generateOTP(request);
    } else if (ActorOperations.VERIFY_OTP.getValue().equals(request.getOperation())) {
      verifyOTP(request);
    } else {
      onReceiveUnsupportedOperation("OTPActor");
    }
  }

  private void generateOTP(Request request) {
    String type = (String) request.getRequest().get(JsonKey.TYPE);
    String key = getKey(type, request);

    String userId = (String) request.getRequest().get(JsonKey.USER_ID);
    if (StringUtils.isNotBlank(userId)) {
      key = OTPUtil.getEmailPhoneByUserId(userId, type);
      type = getType(type);
    }

    rateLimitService.throttleByKey(
        key, new RateLimiter[] {OtpRateLimiter.HOUR, OtpRateLimiter.DAY});

    String otp = null;
    Map<String, Object> details = otpService.getOTPDetails(type, key);
    if (MapUtils.isEmpty(details)) {
      otp = OTPUtil.generateOTP();
      ProjectLogger.log(
          "OTPActor:generateOTP: inserting otp Key = " + key + " OTP = " + otp,
          LoggerEnum.INFO.name());

      otpService.insertOTPDetails(type, key, otp);
    } else {
      otp = (String) details.get(JsonKey.OTP);
    }

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());

    sendOTP(request, otp, key);
  }

  private String getType(String type) {
    switch (type) {
      case JsonKey.PREV_USED_EMAIL:
        return JsonKey.EMAIL;
      case JsonKey.PREV_USED_PHONE:
        return JsonKey.PHONE;
      case JsonKey.EMAIL:
        return JsonKey.EMAIL;
      case JsonKey.PHONE:
        return JsonKey.PHONE;
      case JsonKey.RECOVERY_EMAIL:
        return JsonKey.EMAIL;
      case JsonKey.RECOVERY_PHONE:
        return JsonKey.PHONE;
      default:
        return null;
    }
  }

  private void verifyOTP(Request request) {
    String type = (String) request.getRequest().get(JsonKey.TYPE);
    String key = getKey(type, request);
    String otpInRequest = (String) request.getRequest().get(JsonKey.OTP);

    String userId = (String) request.getRequest().get(JsonKey.USER_ID);
    if (StringUtils.isNotBlank(userId)) {
      key = OTPUtil.getEmailPhoneByUserId(userId, type);
      type = getType(type);
    }

    Map<String, Object> otpDetails = otpService.getOTPDetails(type, key);
    if (MapUtils.isEmpty(otpDetails)) {
      ProjectLogger.log(
          "OTPActor:verifyOTP: Details not found for type = " + type + " key = " + key,
          LoggerEnum.DEBUG);
      ProjectCommonException.throwClientErrorException(ResponseCode.errorInvalidOTP);
    }
    otpService.deleteOtp(type, key);
    String otpInDB = (String) otpDetails.get(JsonKey.OTP);
    if (otpInDB == null || otpInRequest == null || !otpInRequest.equals(otpInDB)) {
      ProjectLogger.log(
          "OTPActor:verifyOTP: OTP mismatch otpInRequest = "
              + otpInRequest
              + " otpInDB = "
              + otpInDB,
          LoggerEnum.DEBUG);
      ProjectCommonException.throwClientErrorException(ResponseCode.errorInvalidOTP);
    }

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
  }

  private void sendOTP(Request request, String otp, String key) {
    Request sendOtpRequest = new Request();
    sendOtpRequest.getRequest().putAll(request.getRequest());
    sendOtpRequest.getRequest().put(JsonKey.KEY, key);
    sendOtpRequest.getRequest().put(JsonKey.OTP, otp);
    sendOtpRequest.setOperation(ActorOperations.SEND_OTP.getValue());
    tellToAnother(sendOtpRequest);
  }

  private String getKey(String type, Request request) {
    String key = (String) request.getRequest().get(JsonKey.KEY);
    if (JsonKey.EMAIL.equalsIgnoreCase(type) && key != null) {
      return key.toLowerCase();
    }
    return key;
  }
}
