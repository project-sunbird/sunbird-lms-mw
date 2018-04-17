package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/**
 * This actor will handle bulk upload operation .
 *
 * @author Amit Kumar
 */

@ActorConfig(tasks = {"bulkUpload", "getBulkOpStatus"}, asyncTasks = {})
public class BulkUploadManagementActor extends BaseActor {

    private static final String CSV_FILE_EXTENSION = ".csv";
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private int userDataSize = 0;
    private int orgDataSize = 0;
    private int batchDataSize = 0;

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, JsonKey.USER);
        // set request id fto thread local...
        ExecutionContext.setRequestId(request.getRequestId());
        if (request.getOperation().equalsIgnoreCase(ActorOperations.BULK_UPLOAD.getValue())) {
            upload(request);
        } else if (request.getOperation()
                .equalsIgnoreCase(ActorOperations.GET_BULK_OP_STATUS.getValue())) {
            getUploadStatus(request);
        } else {
            onReceiveUnsupportedOperation(request.getOperation());
        }
    }

    private void getUploadStatus(Request actorMessage) {
        String processId = (String) actorMessage.getRequest().get(JsonKey.PROCESS_ID);
        DecryptionService decryptionService =
                org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                        .getDecryptionServiceInstance(null);
        Response response = null;
        response = cassandraOperation.getRecordById(bulkDb.getKeySpace(), bulkDb.getTableName(),
                processId);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resList =
                ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE));
        if (!resList.isEmpty()) {
            Map<String, Object> resMap = resList.get(0);
            if ((int) resMap.get(JsonKey.STATUS) == ProjectUtil.BulkProcessStatus.COMPLETED
                    .getValue()) {
                resMap.remove(JsonKey.STATUS);
                resMap.remove(JsonKey.PROCESS_END_TIME);
                resMap.remove(JsonKey.PROCESS_START_TIME);
                resMap.remove(JsonKey.DATA);
                resMap.remove(JsonKey.UPLOADED_BY);
                resMap.remove(JsonKey.UPLOADED_DATE);
                resMap.remove(JsonKey.ORGANISATION_ID);
                resMap.put(JsonKey.PROCESS_ID, resMap.get(JsonKey.ID));
                resMap.remove(JsonKey.ID);
                ObjectMapper mapper = new ObjectMapper();
                Object[] successMap = null;
                Object[] failureMap = null;
                try {
                    if (null != resMap.get(JsonKey.SUCCESS_RESULT)) {
                        successMap = mapper.readValue(
                                decryptionService
                                        .decryptData((String) resMap.get(JsonKey.SUCCESS_RESULT)),
                                Object[].class);
                        resMap.put(JsonKey.SUCCESS_RESULT, successMap);
                    }
                    if (null != resMap.get(JsonKey.FAILURE_RESULT)) {
                        failureMap = mapper.readValue(
                                decryptionService
                                        .decryptData((String) resMap.get(JsonKey.FAILURE_RESULT)),
                                Object[].class);
                        resMap.put(JsonKey.FAILURE_RESULT, failureMap);
                    }
                } catch (IOException e) {
                    ProjectLogger.log(e.getMessage(), e);
                }
                sender().tell(response, self());
            } else {
                response = new Response();
                response.put(JsonKey.RESPONSE,
                        "Operation is still in progress, Please try after some time.");
                sender().tell(response, self());
            }
        } else {
            throw new ProjectCommonException(ResponseCode.invalidProcessId.getErrorCode(),
                    ResponseCode.invalidProcessId.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }

    }

    @SuppressWarnings("unchecked")
    private void upload(Request actorMessage) throws IOException {
        String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
        Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.DATA);
        req.put(JsonKey.CREATED_BY, req.get(JsonKey.CREATED_BY));
        if (((String) req.get(JsonKey.OBJECT_TYPE)).equals(JsonKey.USER)) {
            processBulkUserUpload(req, processId);
        } else if (((String) req.get(JsonKey.OBJECT_TYPE)).equals(JsonKey.ORGANISATION)) {
            processBulkOrgUpload(req, processId);
        } else if (((String) req.get(JsonKey.OBJECT_TYPE)).equals(JsonKey.BATCH)) {
            processBulkBatchEnrollment(req, processId);
        }

    }

    private void processBulkBatchEnrollment(Map<String, Object> req, String processId) {
        File file = new File("bulk.csv");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write((byte[]) req.get(JsonKey.FILE));
        } catch (IOException e) {
            ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor",
                    e);
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                ProjectLogger.log(
                        "Exception Occurred while closing fileInputStream in BulkUploadManagementActor",
                        e);
            }
        }

        List<String[]> batchList = parseCsvFile(file);
        if (null != batchList) {

            if (null != PropertiesCache.getInstance()
                    .getProperty(JsonKey.BULK_UPLOAD_BATCH_DATA_SIZE)) {
                batchDataSize = (Integer.parseInt(PropertiesCache.getInstance()
                        .getProperty(JsonKey.BULK_UPLOAD_BATCH_DATA_SIZE)));
                ProjectLogger
                        .log("bulk upload batch data size read from config file " + batchDataSize);
            }
            if (batchDataSize > 0 && batchList.size() > batchDataSize) {
                throw new ProjectCommonException(ResponseCode.dataSizeError.getErrorCode(),
                        ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(),
                                batchDataSize),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
            if (!batchList.isEmpty()) {
                String[] columns = batchList.get(0);
                validateBatchProperty(columns);
            } else {
                throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                        ResponseCode.csvError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        } else {
            throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                    ResponseCode.csvError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // save csv file to db
        uploadCsvToDB(batchList, processId, null, JsonKey.BATCH,
                (String) req.get(JsonKey.CREATED_BY), null);

    }

    private void processBulkOrgUpload(Map<String, Object> req, String processId)
            throws IOException {

        File file = new File("bulk-" + processId + CSV_FILE_EXTENSION);
        List<String[]> orgList = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write((byte[]) req.get(JsonKey.FILE));
            orgList = parseCsvFile(file);
        } catch (IOException e) {
            ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor",
                    e);
            throw e;
        } finally {
            try {
                if (ProjectUtil.isNotNull(fos)) {
                    fos.close();
                }
                if (ProjectUtil.isNotNull(file)) {
                    file.delete();
                }
            } catch (IOException e) {
                ProjectLogger.log(
                        "Exception Occurred while closing fileInputStream in BulkUploadManagementActor",
                        e);
            }
        }
        if (null != orgList) {
            if (null != PropertiesCache.getInstance()
                    .getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)) {
                orgDataSize = (Integer.parseInt(PropertiesCache.getInstance()
                        .getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)));
                ProjectLogger.log("bulk upload org data size read from config file " + orgDataSize);
            }
            if (orgDataSize > 0 && orgList.size() > orgDataSize) {
                throw new ProjectCommonException(ResponseCode.dataSizeError.getErrorCode(),
                        ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(),
                                orgDataSize),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
            if (!orgList.isEmpty()) {
                String[] columns = orgList.get(0);
                validateOrgProperty(columns);
            } else {
                throw new ProjectCommonException(ResponseCode.dataSizeError.getErrorCode(),
                        ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(),
                                orgDataSize),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        } else {
            throw new ProjectCommonException(ResponseCode.dataSizeError.getErrorCode(), ProjectUtil
                    .formatMessage(ResponseCode.dataSizeError.getErrorMessage(), orgDataSize),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // save csv file to db
        uploadCsvToDB(orgList, processId, null, JsonKey.ORGANISATION,
                (String) req.get(JsonKey.CREATED_BY), null);
    }

    private void validateOrgProperty(String[] property) {
        ArrayList<String> properties = new ArrayList<>(Arrays.asList(JsonKey.ORGANISATION_NAME,
                JsonKey.CHANNEL, JsonKey.IS_ROOT_ORG, JsonKey.PROVIDER, JsonKey.EXTERNAL_ID,
                JsonKey.DESCRIPTION, JsonKey.HOME_URL, JsonKey.ORG_CODE, JsonKey.ORG_TYPE,
                JsonKey.PREFERRED_LANGUAGE, JsonKey.THEME, JsonKey.CONTACT_DETAILS, JsonKey.LOC_ID,
                JsonKey.HASHTAGID));

        for (String key : property) {
            if (!properties.contains(key)) {
                ProjectLogger.log("Invalid Column bulk upload management actor :: " + key);
                throw new ProjectCommonException(ResponseCode.InvalidColumnError.getErrorCode(),
                        ResponseCode.InvalidColumnError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }

    }

    @SuppressWarnings("unchecked")
    private void processBulkUserUpload(Map<String, Object> req, String processId) {

        DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
        String orgId = "";
        Response response = null;
        if (!StringUtils.isBlank((String) req.get(JsonKey.ORGANISATION_ID))) {
            response = cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(),
                    (String) req.get(JsonKey.ORGANISATION_ID));
        } else {
            Map<String, Object> map = new HashMap<>();
            map.put(JsonKey.EXTERNAL_ID, ((String) req.get(JsonKey.EXTERNAL_ID)).toLowerCase());
            map.put(JsonKey.PROVIDER, ((String) req.get(JsonKey.PROVIDER)).toLowerCase());
            response = cassandraOperation.getRecordsByProperties(orgDb.getKeySpace(),
                    orgDb.getTableName(), map);
        }
        List<Map<String, Object>> responseList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

        if (responseList.isEmpty()) {
            throw new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                    ResponseCode.invalidOrgData.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
            orgId = (String) responseList.get(0).get(JsonKey.ID);
        }

        // validate root org id

        String rootOrgId = "";
        Map<String, Object> orgMap = responseList.get(0);
        boolean isRootOrg = false;
        if (null != orgMap.get(JsonKey.IS_ROOT_ORG)) {
            isRootOrg = (boolean) orgMap.get(JsonKey.IS_ROOT_ORG);
        } else {
            isRootOrg = false;
        }
        if (isRootOrg) {
            rootOrgId = orgId;
        } else {
            if (!StringUtils.isBlank((String) orgMap.get(JsonKey.ROOT_ORG_ID))) {
                rootOrgId = (String) orgMap.get(JsonKey.ROOT_ORG_ID);
            } else {
                rootOrgId = JsonKey.DEFAULT_ROOT_ORG_ID;
            }
        }
        // --------------------------------------------------
        File file = new File("bulk.csv");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write((byte[]) req.get(JsonKey.FILE));
        } catch (IOException e) {
            ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor",
                    e);
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                ProjectLogger.log(
                        "Exception Occurred while closing fileInputStream in BulkUploadManagementActor",
                        e);
            }
        }

        List<String[]> userList = parseCsvFile(file);
        if (null != userList) {
            if (null != PropertiesCache.getInstance()
                    .getProperty(JsonKey.BULK_UPLOAD_USER_DATA_SIZE)) {
                userDataSize = (Integer.parseInt(PropertiesCache.getInstance()
                        .getProperty(JsonKey.BULK_UPLOAD_USER_DATA_SIZE)));

                ProjectLogger
                        .log("bulk upload user data size read from config file " + userDataSize);
            }
            if (userDataSize > 0 && userList.size() > userDataSize) {
                throw new ProjectCommonException(ResponseCode.dataSizeError.getErrorCode(),
                        ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(),
                                userDataSize),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
            if (!userList.isEmpty()) {
                String[] columns = userList.get(0);
                validateUserProperty(columns);
            } else {
                throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                        ResponseCode.csvError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        } else {
            throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                    ResponseCode.csvError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // save csv file to db
        uploadCsvToDB(userList, processId, orgId, JsonKey.USER,
                (String) req.get(JsonKey.CREATED_BY), rootOrgId);
    }

    private void uploadCsvToDB(List<String[]> dataList, String processId, String orgId,
            String objectType, String requestedBy, String rootOrgId) {
        List<Map<String, Object>> dataMapList = new ArrayList<>();
        if (dataList.size() > 1) {
            try {
                String[] columnArr = dataList.get(0);
                columnArr = trimColumnAttriutes(columnArr);
                Map<String, Object> dataMap = null;
                for (int i = 1; i < dataList.size(); i++) {
                    dataMap = new HashMap<>();
                    String[] valueArr = dataList.get(i);
                    for (int j = 0; j < valueArr.length; j++) {
                        String value =
                                (valueArr[j].trim().length() == 0 ? null : valueArr[j].trim());
                        dataMap.put(columnArr[j], value);
                    }
                    if (!StringUtils.isBlank(objectType)
                            && objectType.equalsIgnoreCase(JsonKey.USER)) {
                        dataMap.put(JsonKey.REGISTERED_ORG_ID, orgId.trim());
                        dataMap.put(JsonKey.ROOT_ORG_ID, rootOrgId.trim());
                    }
                    dataMapList.add(dataMap);
                }
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e);
                throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                        ResponseCode.csvError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        } else {
            // tell sender that csv file is empty
            throw new ProjectCommonException(ResponseCode.csvError.getErrorCode(),
                    ResponseCode.csvError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // convert userMapList to json string
        Map<String, Object> map = new HashMap<>();

        ObjectMapper mapper = new ObjectMapper();
        try {
            map.put(JsonKey.DATA, mapper.writeValueAsString(dataMapList));
        } catch (IOException e) {
            ProjectLogger.log(e.getMessage(), e);
        }

        map.put(JsonKey.ID, processId);
        map.put(JsonKey.OBJECT_TYPE, objectType);
        map.put(JsonKey.UPLOADED_BY, requestedBy);
        map.put(JsonKey.UPLOADED_DATE, ProjectUtil.getFormattedDate());
        map.put(JsonKey.PROCESS_START_TIME, ProjectUtil.getFormattedDate());
        map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.NEW.getValue());
        Response res =
                cassandraOperation.insertRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
        res.put(JsonKey.PROCESS_ID, processId);
        sender().tell(res, self());
        if (((String) res.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            // send processId for data processing to background job
            Request request = new Request();
            request.put(JsonKey.PROCESS_ID, processId);
            request.setOperation(ActorOperations.PROCESS_BULK_UPLOAD.getValue());
            tellToAnother(request);
        }
    }

    private String[] trimColumnAttriutes(String[] columnArr) {

        for (int i = 0; i < columnArr.length; i++) {
            columnArr[i] = columnArr[i].trim();
        }
        return columnArr;
    }

    private List<String[]> parseCsvFile(File file) {
        CSVReader csvReader = null;
        // Create List for holding objects
        List<String[]> rows = new ArrayList<>();
        try {
            // Reading the csv file
            csvReader = new CSVReader(new FileReader(file), ',', '"', 0);
            String[] nextLine;
            // Read one line at a time
            while ((nextLine = csvReader.readNext()) != null) {
                if (isLineEmpty(nextLine)) {
                    continue;
                }
                List<String> list = new ArrayList<>();
                for (String token : nextLine) {
                    list.add(token);
                }
                rows.add(list.toArray(list.toArray(new String[nextLine.length])));
            }
        } catch (Exception e) {
            ProjectLogger.log("Exception occured while processing csv file : ", e);
        } finally {
            try {
                // closing the reader
                csvReader.close();
                file.delete();
            } catch (Exception e) {
                ProjectLogger.log("Exception occured while closing csv reader : ", e);
            }
        }
        return rows;
    }

    private boolean isLineEmpty(String[] nextLine) {
        for (String str : nextLine) {
            if (StringUtils.isNotEmpty(str)) {
                return false;
            }
        }
        return true;
    }

    private void validateUserProperty(String[] property) {
        ArrayList<String> properties = new ArrayList<>(Arrays.asList(JsonKey.FIRST_NAME,
                JsonKey.COUNTRY_CODE, JsonKey.LAST_NAME, JsonKey.PHONE, JsonKey.COUNTRY_CODE,
                JsonKey.EMAIL, JsonKey.PASSWORD, JsonKey.USERNAME, JsonKey.PROVIDER,
                JsonKey.PHONE_VERIFIED, JsonKey.EMAIL_VERIFIED, JsonKey.ROLES, JsonKey.POSITION,
                JsonKey.GRADE, JsonKey.LOCATION, JsonKey.DOB, JsonKey.GENDER, JsonKey.LANGUAGE,
                JsonKey.PROFILE_SUMMARY, JsonKey.SUBJECT, JsonKey.WEB_PAGES));

        for (String key : property) {
            if (!properties.contains(key)) {
                throw new ProjectCommonException(ResponseCode.InvalidColumnError.getErrorCode(),
                        ResponseCode.InvalidColumnError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }

    }

    private void validateBatchProperty(String[] property) {
        ArrayList<String> properties =
                new ArrayList<>(Arrays.asList(JsonKey.BATCH_ID, JsonKey.USER_IDs));
        for (String key : property) {
            if (!properties.contains(key)) {
                throw new ProjectCommonException(ResponseCode.InvalidColumnError.getErrorCode(),
                        ResponseCode.InvalidColumnError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }

    }

    private Map<String, Object> elasticSearchComplexSearch(Map<String, Object> filters,
            String index, String type) {
        SearchDTO searchDTO = new SearchDTO();
        searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
        return ElasticSearchUtil.complexSearch(searchDTO, index, type);
    }

}
