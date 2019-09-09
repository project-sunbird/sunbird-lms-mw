package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.org.OrganisationClient;
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.bean.ShadowUserUpload;
import org.sunbird.bean.MigrationUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.model.BulkMigrationUser;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * @author anmolgupta
 */
@ActorConfig(
        tasks = {"userBulkMigration"},
        asyncTasks = {}
)
public class UserBulkMigrationActor extends BaseBulkUploadActor {
    private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static CSVReader csvReader;
    public static final int RETRY_COUNT=1;
    public static final String USER_BULK_MIGRATION_FIELD="shadowdbmandatorycolumn";
    private Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private static ObjectMapper mapper=new ObjectMapper();
    private static SystemSetting systemSetting;
    private OrganisationClient organisationClient = new OrganisationClientImpl();
    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.USER);
        ExecutionContext.setRequestId(request.getRequestId());
        String operation = request.getOperation();
        if (operation.equalsIgnoreCase(BulkUploadActorOperation.USER_BULK_MIGRATION.getValue())) {
            uploadCsv(request);
        } else {
            onReceiveUnsupportedOperation("userBulkMigration");
        }
    }

    private void uploadCsv(Request request) throws IOException {
        Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
         systemSetting  =
                systemSettingClient.getSystemSettingByField(
                        getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
                        USER_BULK_MIGRATION_FIELD);
        processCsvBytes(req,request);
    }

    private void processCsvBytes(Map<String,Object>data,Request request) throws IOException {
        Map<String,Object>values= mapper.readValue(systemSetting.getValue(),Map.class);
        String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
        long validationStartTime =System.currentTimeMillis();
        List<MigrationUser>migrationUserList=getMigrationUsers(processId,(byte[])data.get(JsonKey.FILE),values);
        ProjectLogger.log("UserBulkMigrationActor:processRecord: time taken to validate records of size ".concat(migrationUserList.size()+"")+"is(ms): ".concat((System.currentTimeMillis()-validationStartTime)+""),LoggerEnum.INFO.name());
        BulkMigrationUser migrationUser=prepareRecord(request,processId,migrationUserList);
        ProjectLogger.log("UserBulkMigrationActor:processRecord:processing record for number of users ".concat(migrationUserList.size()+""));
        insertRecord(migrationUser);
    }

    private void insertRecord(BulkMigrationUser bulkMigrationUser){
        long insertStartTime=System.currentTimeMillis();
        Map<String,Object>record=mapper.convertValue(bulkMigrationUser,Map.class);
        long createdOn=System.currentTimeMillis();
        record.put(JsonKey.CREATED_ON,new Timestamp(createdOn));
        record.put(JsonKey.LAST_UPDATED_ON,new Timestamp(createdOn));
        Response response=cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), record);
        ProjectLogger.log("UserBulkMigrationActor:insertRecord:time taken by cassandra to insert record of size ".concat(record.size()+"")+"is(ms):".concat((System.currentTimeMillis()-insertStartTime)+""));
        sender().tell(response,self());
    }
    private BulkMigrationUser prepareRecord(Request request,String processID,List<MigrationUser>migrationUserList){
        try {
            String decryptedData=mapper.writeValueAsString(migrationUserList);
            BulkMigrationUser migrationUser=new BulkMigrationUser.BulkMigrationUserBuilder(processID,decryptedData)
                    .setObjectType(JsonKey.MIGRATION_USER_OBJECT)
                    .setUploadedDate(ProjectUtil.getFormattedDate())
                    .setStatus(ProjectUtil.BulkProcessStatus.NEW.getValue())
                    .setRetryCount(RETRY_COUNT)
                    .setTaskCount(migrationUserList.size())
                    .setCreatedBy(getCreatedBy(request))
                    .setUploadedBy(getCreatedBy(request))
                    .build();
                    return migrationUser;
        }catch (Exception e){
            e.printStackTrace();
            ProjectLogger.log("UserBulkMigrationActor:prepareRecord:error occurred while getting preparing record with processId".concat(processID+""),LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.SERVER_ERROR.getErrorCode(),
                    ResponseCode.SERVER_ERROR.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }

    private String getCreatedBy(Request request){
        Map<String,String>data=(Map<String, String>) request.getRequest().get(JsonKey.DATA);
        return MapUtils.isNotEmpty(data)?data.get(JsonKey.CREATED_BY):null;
    }

    private List<MigrationUser> getMigrationUsers(String processId,byte[] fileData,Map<String,Object>fieldsMap){
            Map<String, List<String>> columnsMap = (Map<String, List<String>>) fieldsMap.get(JsonKey.FILE_TYPE_CSV);
            List<String[]> csvData=readCsv(fileData);
            List<String>csvHeaders=getCsvHeadersAsList(csvData);
            List<String>mappedCsvHeaders=mapCsvColumn(csvHeaders);
            List<MigrationUser>migrationUserList=parseCsvRows(getCsvRowsAsList(csvData),mappedCsvHeaders);
            ShadowUserUpload migration = new ShadowUserUpload.ShadowUserUploadBuilder()
                    .setHeaders(csvHeaders)
                    .setMappedHeaders(mappedCsvHeaders)
                    .setProcessId(processId)
                    .setFileData(fileData)
                    .setFileSize(fileData.length+"")
                    .setMandatoryFields(columnsMap.get(JsonKey.MANDATORY_FIELDS))
                    .setSupportedFields(columnsMap.get(JsonKey.SUPPORTED_COlUMNS))
                    .setValues(migrationUserList)
                    .validate();
            ProjectLogger.log("UserBulkMigrationActor:validateRequestAndReturnMigrationUsers: the migration object formed ".concat(migration.toString()));
            return migrationUserList;
    }

    private List<String[]> readCsv(byte[] fileData){
        List<String[]>values=new ArrayList<>();
        try {
            csvReader = getCsvReader(fileData, ',', '"', 0);
            ProjectLogger.log("UserBulkMigrationActor:readCsv:csvReader initialized ".concat(csvReader.toString()),LoggerEnum.ERROR.name());
            values=csvReader.readAll();
        }
        catch (Exception ex) {
            ProjectLogger.log("UserBulkMigrationActor:readCsv:error occurred while getting csvReader",LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.SERVER_ERROR.getErrorCode(),
                    ResponseCode.SERVER_ERROR.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        } finally {
            IOUtils.closeQuietly(csvReader);
        }
        return values;
    }

    private List<String> getCsvHeadersAsList(List<String[]>csvData){
        List<String>headers=new ArrayList<>();
        int CSV_COLUMN_NAMES=0;
        try {
            if(null!=csvData){
                headers.addAll(Arrays.asList(csvData.get(CSV_COLUMN_NAMES)));
                headers.replaceAll(String::toLowerCase);
            }
        }
        catch (Exception ex) {
            ProjectLogger.log("UserBulkMigrationActor:getCsvHeadersAsList:error occurred while getting csvReader",LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.SERVER_ERROR.getErrorCode(),
                    ResponseCode.SERVER_ERROR.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
        return headers;
    }
    private List<String[]> getCsvRowsAsList(List<String[]>csvData){
        return csvData.subList(1,csvData.size());
    }

    private List<String> mapCsvColumn(List<String> csvColumns){
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
            if(column.equalsIgnoreCase(JsonKey.EXTERNAL_USER_ID))
            {
                mappedColumns.add(JsonKey.USER_EXTERNAL_ID);
            }
            if(column.equalsIgnoreCase(JsonKey.EXTERNAL_ORG_ID)){
                mappedColumns.add(JsonKey.ORG_EXTERNAL_ID);
            }
            if(column.equalsIgnoreCase(JsonKey.NAME)){
                mappedColumns.add(JsonKey.FIRST_NAME);
            }
            if(column.equalsIgnoreCase(JsonKey.INPUT_STATUS)){
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

        if(columnAttribute.equalsIgnoreCase(JsonKey.FIRST_NAME)){
            migrationUser.setName((String)value);
        }
        if(columnAttribute.equalsIgnoreCase(JsonKey.INPUT_STATUS))
        {
            migrationUser.setInputStatus((String)value);
        }
    }
    private String getColumnNameByIndex(List<String>mappedHeaders,int index){
        return mappedHeaders.get(index);
    }

}
