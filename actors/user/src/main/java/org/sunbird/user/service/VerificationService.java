package org.sunbird.user.service;

import java.util.Map;

public interface VerificationService {
  Map<String, Object> verifyCode(String verificationCode, String verificationSource);
}
