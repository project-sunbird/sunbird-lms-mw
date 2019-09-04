package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import jodd.util.ArraysUtil;
import org.apache.commons.io.IOUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.bean.Migration;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.validator.user.UserBulkMigrationRequestValidator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

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
            Migration migration = new Migration.MigrationBuilder()
                    .setHeaders(getCsvHeadersAsList(fileData,processId))
                    .setProcessId(processId)
                    .setFileData(fileData)
                    .setFileSize(fileData.length+"")
                    .setMandatoryFields(columnsMap.get(JsonKey.MANDATORY_FIELDS))
                    .setSupportedFields(columnsMap.get(JsonKey.SUPPORTED_COlUMNS))
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
}
