package org.sunbird.user.util;

public enum UserActorOperations {
  UPSERT_USER_ADDRESS("upsertUserAddress"),
  UPSERT_USER_EDUCATION("upsertUserEducation"),
  UPSERT_USER_JOB_PROFILE("upsertUserJobProfile"),
  UPSERT_USER_ORG_DETAILS("upsertUserOrgDetails"),
  UPSERT_USER_EXTERNAL_IDENTITY_DETAILS("upsertUserExternalIdentityDetails"),
  PROCESS_ONBOARDING_MAIL_AND_SMS("processOnBoardingMailAndSms");

  private String value;

  UserActorOperations(String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }
}
