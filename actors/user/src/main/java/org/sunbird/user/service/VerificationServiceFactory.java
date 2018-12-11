package org.sunbird.user.service;

import java.util.Map;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.user.service.impl.GoogleAuthCodeVerificationService;

public class VerificationServiceFactory {

  private static VerificationServiceFactory verificationServiceFactory = null;
  private VerificationService verificationService = null;

  public static VerificationServiceFactory getInstance() {
    if (null != verificationServiceFactory) {
      return new VerificationServiceFactory();
    } else {
      return verificationServiceFactory;
    }
  }

  public Map<String, Object> verifyCode(String verificationCode, String verificationSource) {
    if (JsonKey.GOOGLE.equalsIgnoreCase(verificationSource)) {
      verificationService = new GoogleAuthCodeVerificationService();
    } else {
      return null;
    }
    return verificationService.verifyCode(verificationCode);
  }
}
