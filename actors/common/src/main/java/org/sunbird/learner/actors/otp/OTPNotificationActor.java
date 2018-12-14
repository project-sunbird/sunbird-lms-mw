package org.sunbird.learner.actors.otp;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.OTPUtil;

@ActorConfig(tasks = {}, asyncTasks = { "processOTPMailAndSMS" })
public class OTPNotificationActor extends BaseActor {

	@Override
	public void onReceive(Request request) throws Throwable {

		if (ActorOperations.PROCESS_OTP_MAIL_AND_SMS.getValue().equals(request.getOperation())) {
			processOTPMailAndSMS(request);
		} else {
			onReceiveUnsupportedOperation("OTPActor");
		}
	}

	private void processOTPMailAndSMS(Request request) {
		String type = (String) request.getRequest().get(JsonKey.TYPE);
	    String key = (String) request.getRequest().get(JsonKey.KEY);
	    String otp = (String) request.getRequest().get(JsonKey.OTP);
	    if (JsonKey.EMAIL.equalsIgnoreCase(type)) {
	        sendOTPMail(key, otp);
	      } else if (JsonKey.PHONE.equalsIgnoreCase(type)) {
	    	 sendOTPSMS(key, otp);
	      }
	}

	private void sendOTPSMS(String key, String otp) {
		Map<String,Object> otpMap = new HashMap<>();
		otpMap.put(JsonKey.PHONE, key);
		otpMap.put(JsonKey.OTP, otp);
		otpMap.put("reason", "self sign-up");
		OTPUtil.sendOTPSMS(otpMap);
	}

	private void sendOTPMail(String key, String otp) {
		Map<String,Object> emailTemplateMap = new HashMap<>();
		emailTemplateMap.put(JsonKey.EMAIL, key);
		emailTemplateMap.put(JsonKey.OTP, otp);
		Request emailRequest = OTPUtil.sendOTPMailRequest(emailTemplateMap);
		tellToAnother(emailRequest);
	}

}
