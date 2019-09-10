package org.sunbird.learner.util;

/**
 * UserFlagEnum provides all the flags of user type
 * It contains flagtype and corresponding boolean value, reason for value is to provide flexibility of providing
 * either true or false and maintaining purpose.
 */
public enum UserFlagEnum {
  PHONE_VERIFIED("phoneVerified", true, 1),
  EMAIL_VERIFIED("emailVerified", true, 2),
  STATE_VALIDATED("isStateValidated", true, 4);

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
