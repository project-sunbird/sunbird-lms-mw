package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;

import java.util.HashMap;
import java.util.Map;

public class UserFlagUtil {

  /**
   * This method return int value of the boolean flag
   * @param userFlagType
   * @param flagEnabled
   * @return
   */
  public static int getFlagValue(String userFlagType, boolean flagEnabled) {
    int decimalValue = 0;
    //if phone is verified, value is set to true
    if(userFlagType.equals(UserFlagEnum.PHONE_VERIFIED.getUserFlagType()) &&
            flagEnabled == true) {
      decimalValue = UserFlagEnum.PHONE_VERIFIED.getUserFlagValue();
    } else if (userFlagType.equals(UserFlagEnum.EMAIL_VERIFIED.getUserFlagType()) &&
            flagEnabled == true) {
      //if email is verified, value is set to true
      decimalValue = UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue();
    } else if (userFlagType.equals(UserFlagEnum.STATE_VALIDATED.getUserFlagType()) &&
            flagEnabled == true) {
      //if user is state-validated, value is set to true
      decimalValue = UserFlagEnum.STATE_VALIDATED.getUserFlagValue();
    }
    return decimalValue;
  }

  /**
   * This method returns boolean flags of user for the flagValue
   * @param flagsValue
   * @return
   */
  public static Map<String, Boolean> assignUserFlagValues(int flagsValue) {
    Map<String, Boolean> userFlagMap = new HashMap<>();
    setDefaultValues(userFlagMap);
    if((flagsValue & UserFlagEnum.PHONE_VERIFIED.getUserFlagValue())== UserFlagEnum.PHONE_VERIFIED.getUserFlagValue()) {
      userFlagMap.put(UserFlagEnum.PHONE_VERIFIED.getUserFlagType(), true);
    } if((flagsValue &  UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue())== UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue()) {
      userFlagMap.put(UserFlagEnum.EMAIL_VERIFIED.getUserFlagType(), true);
    } if((flagsValue &  UserFlagEnum.STATE_VALIDATED.getUserFlagValue())== UserFlagEnum.STATE_VALIDATED.getUserFlagValue()) {
      userFlagMap.put(UserFlagEnum.STATE_VALIDATED.getUserFlagType(), true);
    }
    return userFlagMap;
  }

  private static void setDefaultValues(Map<String, Boolean> userFlagMap) {
    userFlagMap.put(JsonKey.EMAIL_VERIFIED, false);
    userFlagMap.put(JsonKey.PHONE_VERIFIED, false);
    userFlagMap.put(JsonKey.STATE_VALIDATED, false);
  }
}
