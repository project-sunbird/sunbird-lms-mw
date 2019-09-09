package org.sunbird.validator.user;

import com.mchange.v1.util.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.bean.ShadowUserUpload;
import org.sunbird.bean.MigrationUser;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.error.Error;
import org.sunbird.error.ErrorDetails;
import org.sunbird.error.IErrorDispatcher;
import org.sunbird.error.ErrorEnum;
import org.sunbird.error.factory.ErrorDispatcherFactory;

import java.util.HashSet;

public class UserBulkMigrationRequestValidator {

    private ShadowUserUpload shadowUserMigration;
    private HashSet<String> emailSet=new HashSet<>();
    private HashSet<String> phoneSet=new HashSet<>();
    private HashSet<String> userExternalIdsSet=new HashSet<>();
    private Error csvRowsErrors=new Error();
    private static final int MAX_ROW_SUPPORTED=20000;


    private UserBulkMigrationRequestValidator(ShadowUserUpload migration) {
        this.shadowUserMigration = migration;
    }
    public static UserBulkMigrationRequestValidator getInstance(ShadowUserUpload migration){
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
        shadowUserMigration.getMandatoryFields().forEach(
                column->{
                    if(!shadowUserMigration.getHeaders().contains(column.toLowerCase())){
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
        shadowUserMigration.getHeaders().forEach(suppColumn->{
            if(!shadowUserMigration.getSupportedFields().contains(suppColumn.toLowerCase())){
                ProjectLogger.log("UserBulkMigrationRequestValidator:supportedColumns: supported column is not present".concat(suppColumn+""), LoggerEnum.ERROR.name());
                throw new ProjectCommonException(
                        ResponseCode.errorUnsupportedField.getErrorCode(),
                        ResponseCode.errorUnsupportedField.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        "Invalid provided column ".concat(suppColumn).concat("supported headers are:").concat(ArrayUtils.stringifyContents(shadowUserMigration.getSupportedFields().toArray())));
            }
        });
    }



    private void csvRows(){
        validateRowsCount();
        shadowUserMigration.getValues().stream().forEach(migrationUser -> {
            int index=shadowUserMigration.getValues().indexOf(migrationUser);
            validateMigrationUser(migrationUser,index);
        });
        if(csvRowsErrors.getErrorsList().size()>0){
            IErrorDispatcher errorDispatcher= ErrorDispatcherFactory.getErrorDispatcher(csvRowsErrors);
            errorDispatcher.dispatchError();
        }
    }

    private void validateRowsCount(){
        int ROW_BEGINNING_INDEX=1;
        if(shadowUserMigration.getValues().size()>=MAX_ROW_SUPPORTED){
            throw new ProjectCommonException(
                    ResponseCode.csvRowsExceeds.getErrorCode(),
                    ResponseCode.csvRowsExceeds.getErrorMessage().concat("supported:"+MAX_ROW_SUPPORTED),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        else if(shadowUserMigration.getValues().size()<ROW_BEGINNING_INDEX){
            throw new ProjectCommonException(
                    ResponseCode.noDataForConsumption.getErrorCode(),
                    ResponseCode.noDataForConsumption.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }
    private void validateMigrationUser(MigrationUser migrationUser,int index) {
      emailAndPhone(migrationUser.getEmail(),migrationUser.getPhone(),index);
      userExternalId(migrationUser.getUserExternalId(),index);
      name(migrationUser.getName(),index);
      orgExternalId(migrationUser.getOrgExternalId(),index);
      channel(migrationUser.getChannel(),index);
      inputStatus(migrationUser.getInputStatus(),index);
    }
    private void addErrorToList(ErrorDetails errorDetails){
        if(errorDetails.getErrorEnum()!=null){
            csvRowsErrors.setError(errorDetails);
        }
    }

    public void emailAndPhone(String email, String phone,int index) {
        ErrorDetails errorDetails=new ErrorDetails();
        errorDetails.setRowId(index);
        if(StringUtils.isBlank(email) && StringUtils.isBlank(phone)){
            errorDetails.setErrorEnum(ErrorEnum.missing);
            errorDetails.setHeader(JsonKey.EMAIL);
        }
        else if(StringUtils.isNotBlank(email)){
            errorDetails.setHeader(JsonKey.EMAIL);
            boolean isEmailValid=ProjectUtil.isEmailvalid(email);
            if(!isEmailValid){
            errorDetails.setErrorEnum(ErrorEnum.invalid);
            }
            if(isEmailValid && !checkDuplicateValueOrAdd(emailSet,email)){
                errorDetails.setErrorEnum(ErrorEnum.duplicate);
            }

        }
        else  if(StringUtils.isNotBlank(phone)){
            errorDetails.setHeader(JsonKey.PHONE);
            boolean isPhoneValid=ProjectUtil.validatePhoneNumber(phone);
            if(!isPhoneValid){
                errorDetails.setErrorEnum(ErrorEnum.invalid);
            }
            if(isPhoneValid && !checkDuplicateValueOrAdd(phoneSet,phone)){
                errorDetails.setErrorEnum(ErrorEnum.duplicate);
            }
        }
        addErrorToList(errorDetails);
    }

    public void userExternalId(String userExternalId,int index) {
        ErrorDetails errorDetails=new ErrorDetails();
        errorDetails.setRowId(index);
        errorDetails.setHeader(JsonKey.USER_EXTERNAL_ID);
        if(StringUtils.isBlank(userExternalId)){
            errorDetails.setErrorEnum(ErrorEnum.missing);
        }
        if(!checkDuplicateValueOrAdd(userExternalIdsSet,userExternalId)){
            errorDetails.setErrorEnum(ErrorEnum.duplicate);
        }
        addErrorToList(errorDetails);
    }

    public void name(String name,int index) {
        validateColumnAndErrorToList(name,index,JsonKey.NAME);
    }

    public void orgExternalId(String orgExternalId,int index) {
        validateColumnAndErrorToList(orgExternalId,index,JsonKey.ORG_EXTERNAL_ID);
    }

    public void channel(String channel,int index) {
        validateColumnAndErrorToList(channel,index,JsonKey.STATE);
    }

    public void inputStatus(String inputStatus,int index) {
        validateColumnAndErrorToList(inputStatus,index,JsonKey.INPUT_STATUS);
    }

    private boolean checkDuplicateValueOrAdd(HashSet<String>identifier,String value){
        return identifier.add(value);
    }

    private void validateColumnAndErrorToList(String column,int rowIndex,String header){
        ErrorDetails errorDetails=new ErrorDetails();
        errorDetails.setRowId(rowIndex);
        errorDetails.setHeader(header);
        if(StringUtils.isBlank(column)){
            errorDetails.setErrorEnum(ErrorEnum.missing);
        }
        if(header.equalsIgnoreCase(JsonKey.INPUT_STATUS)){
            if(!(column.equalsIgnoreCase(JsonKey.ACTIVE)|| column.equalsIgnoreCase(JsonKey.INACTIVE))){
                errorDetails.setErrorEnum(ErrorEnum.invalid);
            }
        }
        addErrorToList(errorDetails);
    }
}
