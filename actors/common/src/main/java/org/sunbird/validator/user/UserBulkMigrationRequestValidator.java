package org.sunbird.validator.user;

import org.sunbird.bean.Migration;
import org.sunbird.common.models.util.JsonKey;
import java.util.List;
import java.util.Map;

public class UserBulkMigrationRequestValidator {

    private byte[] file;
    private  Map<String, List<String>>fieldsConfigurationMap;

    public static UserBulkMigrationRequestValidator getInstance(byte[] file, Map<String, List<String>>fieldsConfigurationMap){
        return new UserBulkMigrationRequestValidator(file,fieldsConfigurationMap);
    }
    private UserBulkMigrationRequestValidator(byte[] file, Map<String, List<String>>fieldsConfigurationMap) {
        this.file=file;
        this.fieldsConfigurationMap=fieldsConfigurationMap;
    }
    public void validate(){
        Migration migration=new Migration.MigrationBuilder().setFileData(file).setFileSize("20mb").setHeaders(getMandatoryField()).build();
    }

    private List<String> getMandatoryField(){
        return fieldsConfigurationMap.get(JsonKey.MANDATORY_FIELDS);
    }
}
