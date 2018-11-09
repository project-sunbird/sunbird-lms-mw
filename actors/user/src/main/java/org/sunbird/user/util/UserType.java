package org.sunbird.user.util;

public enum UserType {
  teacher("teacher"),
  self_signup("self_signup"),
  others("others");

  private String typeName;

  private UserType(String name) {
    this.typeName = name;
  }

  public String getTypeName() {
    return typeName;
  }
}
