package org.sunbird.user.service.impl;

import java.util.Map;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.user.service.GoogleAuthCodeVerificationService;
import org.sunbird.user.service.VerificationService;

public class VerificationServiceImpl implements VerificationService {

  @Override
  public Map<String, Object> verifyCode(String verificationCode, String verificationSource) {
    if (JsonKey.GOOGLE.equalsIgnoreCase(verificationSource)) {
      return GoogleAuthCodeVerificationService.verifyCode(verificationCode);
    }
    return null;
  }
}
