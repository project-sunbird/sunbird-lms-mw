package org.sunbird.validator.user;

import com.mchange.v1.util.ArrayUtils;
import org.sunbird.bean.Migration;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.responsecode.ResponseCode;
public class UserBulkMigrationRequestValidator {

    private Migration migration;

    private UserBulkMigrationRequestValidator(Migration migration) {
        this.migration = migration;
    }
    public static UserBulkMigrationRequestValidator getInstance(Migration migration){
        return new UserBulkMigrationRequestValidator(migration);
    }
    public void validate()
    {
        csvHeader();
    }
    private void csvHeader(){
        mandatoryColumns();
        supportedColumns();
    }
    private void mandatoryColumns(){
        migration.getMandatoryFields().forEach(
                column->{
                    if(!migration.getHeaders().contains(column)){
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
            if(!migration.getSupportedFields().contains(suppColumn)){
                throw new ProjectCommonException(
                        ResponseCode.errorUnsupportedField.getErrorCode(),
                        ResponseCode.errorUnsupportedField.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        "supported header list ".concat(ArrayUtils.stringifyContents(migration.getSupportedFields().toArray())));
            }
        });
    }
    }
