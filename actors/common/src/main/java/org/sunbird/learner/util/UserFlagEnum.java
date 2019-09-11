package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;

/**
 * UserFlagEnum provides all the flags of user type
 * It contains flagtype and corresponding boolean value, reason for value is to provide flexibility of providing
 * either true or false and maintaining purpose.
 */
public enum UserFlagEnum {
  PHONE_VERIFIED(JsonKey.PHONE_VERIFIED, true, 1),
  EMAIL_VERIFIED(JsonKey.EMAIL_VERIFIED, true, 2),
  STATE_VALIDATED(JsonKey.STATE_VALIDATED, true, 4);

  private String userFlagType;
  private boolean flagEnabled;
  private int userFlagValue;

  UserFlagEnum(String userFlagType, boolean flagEnabled, int userFlagValue) {
    this.userFlagType = userFlagType;
    this.flagEnabled = flagEnabled;
    this.userFlagValue = userFlagValue;
  }

  public int getUserFlagValue() {
    return userFlagValue;
  }

  public String getUserFlagType() {
    return userFlagType;
  }

  public boolean isFlagEnabled() {
    return flagEnabled;
  }

}
