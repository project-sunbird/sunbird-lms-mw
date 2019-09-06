package org.sunbird.validator.user;

public interface IUserMigrationRequestValidator {

    void emailAndPhone(String email,String phone);
    void userExternalId(String userExternalId);
    void name(String name);
    void orgExternalId(String orgExternalId);
    void channel(String channel);
    void inputStatus(boolean inputStatus);

}
