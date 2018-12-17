package org.sunbird.learner.actors.otp.dao;

import java.util.Map;

public interface OTPDao {

  /**
   * Fetch OTP details based on type (phone / email) and key.
   *
   * @param type Type of key (phone / email)
   * @param key Phone number or email address
   * @return OTP details
   */
  Map<String, Object> getOTPDetailsByKey(String type, String key);

  /**
   * Insert OTP details for given type (phone / email) and key
   *
   * @param type Type of key (phone / email)
   * @param key Phone number or email address
   * @param otp Generated OTP
   */
  void insertOTPDetails(String type, String key, String otp);

}
