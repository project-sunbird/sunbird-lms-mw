package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;

import java.util.HashMap;
import java.util.Map;

public class UserFlagUtil {

  /**
   * This method return int value of the boolean flag
   * @param userFlagType
   * @param isFlagEnabled
   * @return
   */
  public static int getFlagValue(String userFlagType, boolean isFlagEnabled) {
    int decimalValue = 0;
    if(userFlagType.equals(UserFlagEnum.PHONE_VERIFIED.getUserFlagType()) &&
            isFlagEnabled== UserFlagEnum.PHONE_VERIFIED.isFlagEnabled()) {
      decimalValue = UserFlagEnum.PHONE_VERIFIED.getUserFlagValue();
    } else if (userFlagType.equals(UserFlagEnum.EMAIL_VERIFIED.getUserFlagType()) &&
            isFlagEnabled== UserFlagEnum.EMAIL_VERIFIED.isFlagEnabled()) {
      decimalValue = UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue();
    } else if (userFlagType.equals(UserFlagEnum.STATE_VALIDATED.getUserFlagType()) &&
            isFlagEnabled== UserFlagEnum.STATE_VALIDATED.isFlagEnabled()) {
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
      userFlagMap.put(UserFlagEnum.PHONE_VERIFIED.getUserFlagType(), UserFlagEnum.PHONE_VERIFIED.isFlagEnabled());
    } if((flagsValue &  UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue())== UserFlagEnum.EMAIL_VERIFIED.getUserFlagValue()) {
      userFlagMap.put(UserFlagEnum.EMAIL_VERIFIED.getUserFlagType(), UserFlagEnum.EMAIL_VERIFIED.isFlagEnabled());
    } if((flagsValue &  UserFlagEnum.STATE_VALIDATED.getUserFlagValue())== UserFlagEnum.STATE_VALIDATED.getUserFlagValue()) {
      userFlagMap.put(UserFlagEnum.STATE_VALIDATED.getUserFlagType(), UserFlagEnum.STATE_VALIDATED.isFlagEnabled());
    }
    return userFlagMap;
  }

  private static void setDefaultValues(Map<String, Boolean> userFlagMap) {
    userFlagMap.put(JsonKey.EMAIL_VERIFIED, false);
    userFlagMap.put(JsonKey.PHONE_VERIFIED, false);
    userFlagMap.put(JsonKey.STATE_VALIDATED, false);
  }
}
