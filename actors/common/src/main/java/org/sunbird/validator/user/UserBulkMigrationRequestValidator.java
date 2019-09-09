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
import org.sunbird.error.CsvError;
import org.sunbird.error.CsvRowErrorDetails;
import org.sunbird.error.IErrorDispatcher;
import org.sunbird.error.ErrorEnum;
import org.sunbird.error.factory.ErrorDispatcherFactory;

import java.util.HashSet;

/**
 * this class will validate the csv file for shadow db
 * @author anmolgupta
 */
public class UserBulkMigrationRequestValidator {

    private ShadowUserUpload shadowUserMigration;
    private HashSet<String> emailSet=new HashSet<>();
    private HashSet<String> phoneSet=new HashSet<>();
    private HashSet<String> userExternalIdsSet=new HashSet<>();
    private CsvError csvRowsErrors=new CsvError();
    private static final int MAX_ROW_SUPPORTED=20000;


    private UserBulkMigrationRequestValidator(ShadowUserUpload migration) {
        this.shadowUserMigration = migration;
    }
    public static UserBulkMigrationRequestValidator getInstance(ShadowUserUpload migration){
        return new UserBulkMigrationRequestValidator(migration);
    }
    public void validate()
    {
        checkCsvHeader();
        checkCsvRows();
    }
    private void checkCsvHeader(){
        checkMandatoryColumns();
        checkSupportedColumns();
    }
    private void checkMandatoryColumns(){
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

    private void checkSupportedColumns(){
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



    private void checkCsvRows(){
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
        checkEmailAndPhone(migrationUser.getEmail(),migrationUser.getPhone(),index);
        checkUserExternalId(migrationUser.getUserExternalId(),index);
        checkName(migrationUser.getName(),index);
        checkOrgExternalId(migrationUser.getOrgExternalId(),index);
        checkChannel(migrationUser.getChannel(),index);
        checkInputStatus(migrationUser.getInputStatus(),index);
    }

    private void addErrorToList(CsvRowErrorDetails errorDetails){
        csvRowsErrors.setError(errorDetails);

    }

    public void checkEmailAndPhone(String email, String phone,int index) {
        CsvRowErrorDetails errorDetails=new CsvRowErrorDetails();
        errorDetails.setRowId(index);
        boolean isEmailBlank=StringUtils.isBlank(email);
        boolean isPhoneBlank=StringUtils.isBlank(phone);

        if(isEmailBlank && isPhoneBlank){
            errorDetails.setErrorEnum(ErrorEnum.missing);
            errorDetails.setHeader(JsonKey.EMAIL);
        }
        else if(!isEmailBlank){
            errorDetails.setHeader(JsonKey.EMAIL);
            boolean isEmailValid=ProjectUtil.isEmailvalid(email);
            if(!isEmailValid){
            errorDetails.setErrorEnum(ErrorEnum.invalid);
            }
            if(isEmailValid){
                if(!emailSet.add(email))
                errorDetails.setErrorEnum(ErrorEnum.duplicate);
            }
        }
        else  if(!isPhoneBlank){
            errorDetails.setHeader(JsonKey.PHONE);
            boolean isPhoneValid=ProjectUtil.validatePhoneNumber(phone);
            if(!isPhoneValid){
                errorDetails.setErrorEnum(ErrorEnum.invalid);
            }
            if(isPhoneValid){
                if(!phoneSet.add(phone)) {
                    errorDetails.setErrorEnum(ErrorEnum.duplicate);
                }
            }
        }
        if(errorDetails.getErrorEnum()!=null) {
            addErrorToList(errorDetails);
        }
    }

    public void checkUserExternalId(String userExternalId,int index) {
        CsvRowErrorDetails errorDetails=new CsvRowErrorDetails();
        errorDetails.setRowId(index);
        errorDetails.setHeader(JsonKey.USER_EXTERNAL_ID);
        if(StringUtils.isBlank(userExternalId)){
            errorDetails.setErrorEnum(ErrorEnum.missing);
        }
        if(!userExternalIdsSet.add(userExternalId)){
            errorDetails.setErrorEnum(ErrorEnum.duplicate);
        }
        if (errorDetails.getErrorEnum() != null) {
            addErrorToList(errorDetails);
        }
    }

    public void checkName(String name,int index) {
        checkValue(name,index,JsonKey.NAME);
    }

    public void checkOrgExternalId(String orgExternalId,int index) {
        checkValue(orgExternalId,index,JsonKey.ORG_EXTERNAL_ID);
    }

    public void checkChannel(String channel,int index) {
        checkValue(channel,index,JsonKey.STATE);
    }

    public void checkInputStatus(String inputStatus,int index) {
        checkValue(inputStatus,index,JsonKey.INPUT_STATUS);
        if (!(inputStatus.equalsIgnoreCase(JsonKey.ACTIVE) ||
                inputStatus.equalsIgnoreCase(JsonKey.INACTIVE))) {
            CsvRowErrorDetails errorDetails=new CsvRowErrorDetails();
            errorDetails.setRowId(index);
            errorDetails.setHeader(JsonKey.INPUT_STATUS);
            errorDetails.setErrorEnum(ErrorEnum.invalid);
            addErrorToList(errorDetails);
        }
    }
    private void checkValue(String column, int rowIndex, String header){
        if(StringUtils.isBlank(column)) {
            CsvRowErrorDetails errorDetails=new CsvRowErrorDetails();
            errorDetails.setRowId(rowIndex);
            errorDetails.setHeader(header);
            errorDetails.setErrorEnum(ErrorEnum.missing);
            addErrorToList(errorDetails);
        }
    }
}
