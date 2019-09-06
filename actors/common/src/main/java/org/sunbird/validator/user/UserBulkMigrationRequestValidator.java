package org.sunbird.validator.user;

import com.mchange.v1.util.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.bean.Migration;
import org.sunbird.bean.MigrationUser;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.HashSet;

public class UserBulkMigrationRequestValidator  implements IUserMigrationRequestValidator{

    private Migration migration;
    private HashSet<String> emailSet=new HashSet<>();
    private HashSet<String> phoneSet=new HashSet<>();
    private HashSet<String> userExternalIdsSet=new HashSet<>();

    private UserBulkMigrationRequestValidator(Migration migration) {
        this.migration = migration;
    }
    public static UserBulkMigrationRequestValidator getInstance(Migration migration){
        return new UserBulkMigrationRequestValidator(migration);
    }
    public void validate()
    {
        csvHeader();
        csvRows();
    }
    private void csvHeader(){
        mandatoryColumns();
        supportedColumns();
    }
    private void mandatoryColumns(){
        migration.getMandatoryFields().forEach(
                column->{
                    if(!migration.getHeaders().contains(column.toLowerCase())){
                        ProjectLogger.log("UserBulkMigrationRequestValidator:mandatoryColumns: mandatory column is not present".concat(column+""), LoggerEnum.ERROR.name());
                        throw new ProjectCommonException(
                                ResponseCode.mandatoryParamsMissing.getErrorCode(),
                                ResponseCode.mandatoryParamsMissing.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode(),
                                column);
                    }
                }
        );
        }

    private void supportedColumns(){
        migration.getHeaders().forEach(suppColumn->{
            if(!migration.getSupportedFields().contains(suppColumn.toLowerCase())){
                ProjectLogger.log("UserBulkMigrationRequestValidator:supportedColumns: supported column is not present".concat(suppColumn+""), LoggerEnum.ERROR.name());
                throw new ProjectCommonException(
                        ResponseCode.errorUnsupportedField.getErrorCode(),
                        ResponseCode.errorUnsupportedField.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        "Invalid provided column ".concat(suppColumn).concat("supported headers are:").concat(ArrayUtils.stringifyContents(migration.getSupportedFields().toArray())));
            }
        });
    }



    private void csvRows(){
        migration.getValues().stream().forEach(migrationUser -> {
            validateMigrationUser(migrationUser);
        });
    }



    private void validateMigrationUser(MigrationUser migrationUser) {

    }

    @Override
    public void emailAndPhone(String email, String phone) {

        if(StringUtils.isNotBlank(email)|| StringUtils.isNotBlank(phone)){

        }

    }

    @Override
    public void userExternalId(String userExternalId) {

    }

    @Override
    public void name(String name) {

    }

    @Override
    public void orgExternalId(String orgExternalId) {

    }

    @Override
    public void channel(String channel) {

    }

    @Override
    public void inputStatus(boolean inputStatus) {

    }


}
