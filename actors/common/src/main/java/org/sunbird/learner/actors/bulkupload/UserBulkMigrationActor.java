package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.validator.user.UserBulkMigrationRequestValidator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ActorConfig(
        tasks = {"userBulkMigration"},
        asyncTasks = {}
)
public class UserBulkMigrationActor extends BaseBulkUploadActor {
    private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
    public static final String USER_BULK_MIGRATION_FIELD="shadowdbmandatorycolumn";
    private String[] bulkUserAllowedFields = {
            JsonKey.FIRST_NAME,
            JsonKey.LAST_NAME,
            JsonKey.PHONE,
            JsonKey.COUNTRY_CODE,
            JsonKey.EMAIL,
            JsonKey.USERNAME,
            JsonKey.PHONE_VERIFIED,
            JsonKey.EMAIL_VERIFIED,
            JsonKey.ROLES,
            JsonKey.POSITION,
            JsonKey.GRADE,
            JsonKey.LOCATION,
            JsonKey.DOB,
            JsonKey.GENDER,
            JsonKey.LANGUAGE,
            JsonKey.PROFILE_SUMMARY,
            JsonKey.SUBJECT,
            JsonKey.WEB_PAGES,
            JsonKey.EXTERNAL_ID_PROVIDER,
            JsonKey.EXTERNAL_ID,
            JsonKey.EXTERNAL_ID_TYPE,
            JsonKey.EXTERNAL_IDS,
            JsonKey.USER_ID,
            JsonKey.ORG_ID
    };

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
        Map<System,Object>values=new HashMap<>();
        ObjectMapper mapper=new ObjectMapper();
        values= mapper.readValue(systemSetting.getValue(),Map.class);
        Map<String,List<String>>sp=(Map<String, List<String>>) values.get(JsonKey.FILE_TYPE_CSV);
        UserBulkMigrationRequestValidator.getInstance((byte[])req.get(JsonKey.FILE),sp).validate();
        Response response=new Response();
        response.put("hello",systemSetting.getValue());
        sender().tell(response,self());
//        if (dataObject != null) {
//            supportedColumnsMap =
//                    ((Map<String, Object>) ((Map<String, Object>) dataObject).get("supportedColumns"));
//            List<String> supportedColumnsList = new ArrayList<>();
//            supportedColumnsLowerCaseMap =
//                    supportedColumnsMap
//                            .entrySet()
//                            .stream()
//                            .collect(
//                                    Collectors.toMap(
//                                            entry -> (entry.getKey()).toLowerCase(), entry -> entry.getValue()));
//
//            Map<String, Object> internalNamesLowerCaseMap = new HashMap<>();
//            supportedColumnsMap.forEach(
//                    (String k, Object v) -> {
//                        internalNamesLowerCaseMap.put(v.toString().toLowerCase(), v.toString());
//                    });
//            supportedColumnsLowerCaseMap.putAll(internalNamesLowerCaseMap);
//            supportedColumnsLowerCaseMap.forEach(
//                    (key, value) -> {
//                        supportedColumnsList.add(key);
//                        supportedColumnsList.add((String) value);
//                    });
//            List<String> mandatoryColumns =
//                    (List<String>) (((Map<String, Object>) dataObject).get("mandatoryColumns"));
//            validateFileHeaderFields(
//                    req,
//                    supportedColumnsList.toArray(new String[supportedColumnsList.size()]),
//                    false,
//                    true,
//                    mandatoryColumns,
//                    supportedColumnsLowerCaseMap);
//
//        } else {
//            validateFileHeaderFields(req, bulkUserAllowedFields, false);
//        }
//        BulkUploadProcess bulkUploadProcess =
//                handleUpload(JsonKey.USER, (String) req.get(JsonKey.CREATED_BY));
//        processUserBulkUpload(
//                req, bulkUploadProcess.getId(), bulkUploadProcess, supportedColumnsLowerCaseMap);
    }

    private void processUserBulkUpload(
            Map<String, Object> req,
            String processId,
            BulkUploadProcess bulkUploadProcess,
            Map<String, Object> supportedColumnsMap)
            throws IOException {
        byte[] fileByteArray = null;
        if (null != req.get(JsonKey.FILE)) {
            fileByteArray = (byte[]) req.get(JsonKey.FILE);
        }
        Integer recordCount =
                validateAndParseRecords(fileByteArray, processId, new HashMap(), supportedColumnsMap, true);
        processBulkUpload(
                recordCount,
                processId,
                bulkUploadProcess,
                BulkUploadActorOperation.USER_BULK_UPLOAD_BACKGROUND_JOB.getValue(),
                bulkUserAllowedFields);
    }


}
