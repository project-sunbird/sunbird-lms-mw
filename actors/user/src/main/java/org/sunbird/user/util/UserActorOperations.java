package org.sunbird.user.util;

public enum UserActorOperations {
  UPSERT_USER_ADDRESS("upsertUserAddress"),
  UPSERT_USER_EDUCATION("upsertUserEducation"),
  UPSERT_USER_JOB_PROFILE("upsertUserJobProfile"),
  UPSERT_USER_ORG_DETAILS("upsertUserOrgDetails"),
  UPSERT_USER_EXTERNAL_IDENTITY_DETAILS("upsertUserExternalIdentityDetails"),
  PROCESS_ONBOARDING_MAIL_AND_SMS("processOnBoardingMailAndSms"),
  SAVE_USER_ATTRIBUTES("saveUserAttributes"),
  UPSERT_USER_DETAILS_TO_ES("upsertUserDetailsToES"),
  UPSERT_USER_ADDRESS_TO_ES("upsertUserAddressToES"),
  UPSERT_USER_EDUCATION_TO_ES("upsertUserEducationToES"),
  UPSERT_USER_JOB_PROFILE_TO_ES("upsertUserJobProfileToES"),
  UPSERT_USER_ORG_DETAILS_TO_ES("upsertUserOrgDetailsToES");

  private String value;

  UserActorOperations(String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }
}
