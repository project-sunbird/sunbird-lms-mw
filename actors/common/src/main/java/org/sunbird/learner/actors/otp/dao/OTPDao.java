/** */
package org.sunbird.learner.actors.otp.dao;

import java.util.Map;

/** @author Rahul Kumar */
public interface OTPDao {

  /**
   * Fetches otp details based on type and key
   *
   * @param type
   * @param key
   * @return map of the otp details from db
   */
  Map<String, Object> getOTPDetailsByKey(String type, String key);

  /**
   * Add a new OTP detail into DB based on type and key
   *
   * @param type
   * @param key
   * @param otp
   */
  void insertOTPDetails(String type, String key, String otp);
}
