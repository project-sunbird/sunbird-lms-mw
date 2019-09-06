package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.commons.io.IOUtils;
import org.stringtemplate.v4.ST;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.bean.Migration;
import org.sunbird.bean.MigrationUser;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ActorConfig(
        tasks = {"userBulkMigration"},
        asyncTasks = {}
)
public class UserBulkMigrationActor extends BaseBulkUploadActor {
    private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
    private static CSVReader csvReader;
    public static final String USER_BULK_MIGRATION_FIELD="shadowdbmandatorycolumn";
    private static ObjectMapper mapper=new ObjectMapper();

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.USER);
        ExecutionContext.setRequestId(request.getRequestId());
        String operation = request.getOperation();
        if (operation.equalsIgnoreCase(BulkUploadActorOperation.USER_BULK_MIGRATION.getValue())) {
            upload(request);
        } else {
            onReceiveUnsupportedOperation("userBulkMigration");
        }
    }

    private void upload(Request request) throws IOException {
        Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
        SystemSetting systemSetting  =
                systemSettingClient.getSystemSettingByField(
                        getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
                        USER_BULK_MIGRATION_FIELD);
        Map<String,Object>values= mapper.readValue(systemSetting.getValue(),Map.class);
        validateRequest((byte[])req.get(JsonKey.FILE),values);
        Response response=new Response();
        response.put("hello",systemSetting.getValue());
        sender().tell(response,self());

    }

    private void validateRequest(byte[] fileData,Map<String,Object>fieldsMap){
            String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
            Map<String, List<String>> columnsMap = (Map<String, List<String>>) fieldsMap.get(JsonKey.FILE_TYPE_CSV);
            List<String>mappedCsvHeaders=mappedCsvColumn(getCsvHeadersAsList(fileData,processId));
            List<MigrationUser>migrationUserList=parseCsvRows(getCsvRowsAsList(fileData),mappedCsvHeaders);
            Migration migration = new Migration.MigrationBuilder()
                    .setHeaders(mappedCsvHeaders)
                    .setProcessId(processId)
                    .setFileData(fileData)
                    .setFileSize(fileData.length+"")
                    .setMandatoryFields(columnsMap.get(JsonKey.MANDATORY_FIELDS))
                    .setSupportedFields(columnsMap.get(JsonKey.SUPPORTED_COlUMNS))
                    .setValues(migrationUserList)
                    .build();
            System.out.println("the migration object is "+ migration.toString());
    }

    private List<String> getCsvHeadersAsList(byte[] fileData,String processId){
        List<String>headers=new ArrayList<>();
        try {
            csvReader = getCsvReader(fileData, ',', '"', 0);
            ProjectLogger.log("UserBulkMigrationActor:getCsvHeadersAsList:csvReader initialized ".concat(csvReader.toString()),LoggerEnum.ERROR.name());
            headers.addAll(Arrays.asList(csvReader.readNext()));
            headers.replaceAll(String::toLowerCase);
        }
        catch (Exception ex) {
            ProjectLogger.log("UserBulkMigrationActor:getCsvHeadersAsList:error occurred while getting csvReader",LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.SERVER_ERROR.getErrorCode(),
                    ResponseCode.SERVER_ERROR.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        } finally {
            IOUtils.closeQuietly(csvReader);
        }
        return headers;
    }
    private List<String[]> getCsvRowsAsList(byte[] fileData){
        List<String[]>values=new ArrayList<>();
        try {
            csvReader = getCsvReader(fileData, ',', '"', 1);
            ProjectLogger.log("UserBulkMigrationActor:getCsvRowsAsList:csvReader initialized ".concat(csvReader.toString()),LoggerEnum.ERROR.name());
            values=csvReader.readAll();
        }
        catch (Exception ex) {
            ProjectLogger.log("UserBulkMigrationActor:getCsvRowsAsList:error occurred while getting csvReader",LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.SERVER_ERROR.getErrorCode(),
                    ResponseCode.SERVER_ERROR.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        } finally {
            IOUtils.closeQuietly(csvReader);
        }
        return values;
    }

    private List<String> mappedCsvColumn(List<String> csvColumns){
        List<String> mappedColumns=new ArrayList<>();
        csvColumns.forEach(column->{
            if(column.equalsIgnoreCase(JsonKey.EMAIL)){
                mappedColumns.add(column);
            }
            if (column.equalsIgnoreCase(JsonKey.PHONE)) {
                 mappedColumns.add(column);
            }
            if(column.equalsIgnoreCase(JsonKey.STATE)){
                mappedColumns.add(JsonKey.CHANNEL);
            }
            if(column.equalsIgnoreCase("ext user id"))
            {
                mappedColumns.add(JsonKey.USER_EXTERNAL_ID);
            }
            if(column.equalsIgnoreCase("ext org id")){
                mappedColumns.add(JsonKey.ORG_EXTERNAL_ID);
            }
            if(column.equalsIgnoreCase(JsonKey.NAME)){
                mappedColumns.add(JsonKey.FIRST_NAME);
            }
            if(column.equalsIgnoreCase("input status")){
                mappedColumns.add(column);
            }
        });
      return mappedColumns;

    }

    private List<MigrationUser> parseCsvRows(List<String[]> values,List<String>mappedHeaders){
        List<MigrationUser> migrationUserList=new ArrayList<>();
        values.stream().forEach(row->{
            MigrationUser migrationUser=new MigrationUser();
            for(int i=0;i<row.length;i++){
                String columnName=getColumnNameByIndex(mappedHeaders,i);
                setFieldToMigrationUserObject(migrationUser,columnName,row[i]);
            }
            migrationUserList.add(migrationUser);
        });
        return migrationUserList;
    }

    private void setFieldToMigrationUserObject(MigrationUser migrationUser,String columnAttribute,Object value){

        if(columnAttribute.equalsIgnoreCase(JsonKey.EMAIL)){
            String email=(String)value;
            migrationUser.setEmail(email);
        }
        if(columnAttribute.equalsIgnoreCase(JsonKey.PHONE)){
            String phone=(String)value;
            migrationUser.setPhone(phone);
        }
        if(columnAttribute.equalsIgnoreCase(JsonKey.CHANNEL)){
            String state=(String)value;
            migrationUser.setChannel(state);
        }

        if(columnAttribute.equalsIgnoreCase(JsonKey.ORG_EXTERNAL_ID)){
            migrationUser.setOrgExternalId((String)value);
        }
        if(columnAttribute.equalsIgnoreCase(JsonKey.USER_EXTERNAL_ID)){
            migrationUser.setUserExternalId((String)value);
        }

        if(columnAttribute.equalsIgnoreCase(JsonKey.NAME)){
            migrationUser.setName((String)value);
        }
        if(columnAttribute.equalsIgnoreCase("input status"))
        {
            migrationUser.setInputStatus(Boolean.parseBoolean(((String) value).toLowerCase()));

        }
    }


    private String getColumnNameByIndex(List<String>mappedHeaders,int index){
        return mappedHeaders.get(index);
    }
}
