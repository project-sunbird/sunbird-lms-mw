package org.sunbird.learner.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.SMSFactory;

import com.warrenstrange.googleauth.GoogleAuthenticator;
import com.warrenstrange.googleauth.GoogleAuthenticatorConfig;
import com.warrenstrange.googleauth.GoogleAuthenticatorKey;
import com.warrenstrange.googleauth.KeyRepresentation;

public final class OTPUtil {
	
  private static final int MINIMUM_OTP_SIZE = 6;

  private OTPUtil() {}

  public static String generateOTP() {
	String otpSize = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_OTP_SIZE);
	int codeDigit = StringUtils.isBlank(otpSize)?MINIMUM_OTP_SIZE:Integer.valueOf(otpSize);
	GoogleAuthenticatorConfig config =  new GoogleAuthenticatorConfig.GoogleAuthenticatorConfigBuilder().setCodeDigits(codeDigit).setKeyRepresentation(KeyRepresentation.BASE64).build();
    GoogleAuthenticator gAuth = new GoogleAuthenticator(config);
    GoogleAuthenticatorKey key = gAuth.createCredentials();
    String secret = key.getKey();
    int code = gAuth.getTotpPassword(secret);
    return String.valueOf(code);
  }
  
	public static void sendOTPSMS(Map<String, Object> otpMap) {
		if (StringUtils.isBlank((String) otpMap.get(JsonKey.PHONE))) {
			return;
		}
		Map<String, String> smsTemplate = new HashMap<>();
		smsTemplate.put(JsonKey.OTP, (String) otpMap.get(JsonKey.OTP));

		String sms = ProjectUtil.getOTPSMSBody(smsTemplate);

		ProjectLogger.log("SMS text : " + sms, LoggerEnum.INFO);
		String countryCode = "";
		if (StringUtils.isBlank((String) otpMap.get(JsonKey.COUNTRY_CODE))) {
			countryCode = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE);
		} else {
			countryCode = (String) otpMap.get(JsonKey.COUNTRY_CODE);
		}
		ISmsProvider smsProvider = SMSFactory.getInstance("91SMS");
		ProjectLogger.log("SMS OTP text : " + sms + " with phone " + (String) otpMap.get(JsonKey.PHONE),
				LoggerEnum.INFO.name());
		boolean response = smsProvider.send((String) otpMap.get(JsonKey.PHONE), countryCode, sms);
		ProjectLogger.log("Response from smsProvider : " + response, LoggerEnum.INFO);
		if (response) {
			ProjectLogger.log("OTP Message sent successfully to ." + (String) otpMap.get(JsonKey.PHONE),
					LoggerEnum.INFO.name());
		} else {
			ProjectLogger.log("OTP Message failed for ." + (String) otpMap.get(JsonKey.PHONE), LoggerEnum.INFO.name());
		}
	}
	
	public static Request sendOTPMailRequest(Map<String, Object> emailTemplateMap) {
	    Request request = null;
	    // TODO Implement flow for generating request to send otp in email
	    return request;
	  }
}
