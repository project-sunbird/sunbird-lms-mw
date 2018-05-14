package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessTaskDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessTaskDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle bulk upload operation .
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {"bulkUpload", "getBulkOpStatus"},
  asyncTasks = {}
)
public class BulkUploadManagementActor extends BaseBulkUploadActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
  private int userDataSize = 0;
  private int orgDataSize = 0;
  private int batchDataSize = 0;
  BulkUploadProcessTaskDao bulkUploadProcessTaskDao = new BulkUploadProcessTaskDaoImpl();
  private ObjectMapper mapper = new ObjectMapper();

  private String[] bulkUserAllowedFields = {
    JsonKey.FIRST_NAME,
    JsonKey.COUNTRY_CODE,
    JsonKey.LAST_NAME,
    JsonKey.PHONE,
    JsonKey.COUNTRY_CODE,
    JsonKey.EMAIL,
    JsonKey.PASSWORD,
    JsonKey.USERNAME,
    JsonKey.PROVIDER,
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
    JsonKey.WEB_PAGES
  };

  private String[] bulkBatchAllowedFields = {JsonKey.BATCH_ID, JsonKey.USER_IDs};
  private String[] bulkOrgAllowedFields = {
    JsonKey.ORGANISATION_NAME,
    JsonKey.CHANNEL,
    JsonKey.IS_ROOT_ORG,
    JsonKey.PROVIDER,
    JsonKey.EXTERNAL_ID,
    JsonKey.DESCRIPTION,
    JsonKey.HOME_URL,
    JsonKey.ORG_CODE,
    JsonKey.ORG_TYPE,
    JsonKey.PREFERRED_LANGUAGE,
    JsonKey.THEME,
    JsonKey.CONTACT_DETAILS,
    JsonKey.LOC_ID,
    JsonKey.HASHTAGID,
    JsonKey.LOCATION_CODE
  };

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    // set request id fto thread local...
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.BULK_UPLOAD.getValue())) {
      upload(request);
    } else if (request
        .getOperation()
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
    List<String> fields = Arrays.asList(JsonKey.ID, JsonKey.STATUS, JsonKey.OBJECT_TYPE);
    response =
        cassandraOperation.getRecordById(
            bulkDb.getKeySpace(), bulkDb.getTableName(), processId, fields);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resList =
        ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE));
    if (!resList.isEmpty()) {
      Map<String, Object> resMap = resList.get(0);
      String objectType = (String) resMap.get(JsonKey.OBJECT_TYPE);
      if ((int) resMap.get(JsonKey.STATUS) == ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
        if (!(JsonKey.LOCATION.equalsIgnoreCase(objectType))) {
          resMap.put(JsonKey.PROCESS_ID, resMap.get(JsonKey.ID));
          ProjectUtil.removeUnwantedFields(resMap, JsonKey.STATUS, JsonKey.ID);
          ObjectMapper mapper = new ObjectMapper();
          Object[] successMap = null;
          Object[] failureMap = null;
          try {
            if (null != resMap.get(JsonKey.SUCCESS_RESULT)) {
              successMap =
                  mapper.readValue(
                      decryptionService.decryptData((String) resMap.get(JsonKey.SUCCESS_RESULT)),
                      Object[].class);
              resMap.put(JsonKey.SUCCESS_RESULT, successMap);
            }
            if (null != resMap.get(JsonKey.FAILURE_RESULT)) {
              failureMap =
                  mapper.readValue(
                      decryptionService.decryptData((String) resMap.get(JsonKey.FAILURE_RESULT)),
                      Object[].class);
              resMap.put(JsonKey.FAILURE_RESULT, failureMap);
            }
          } catch (IOException e) {
            ProjectLogger.log(e.getMessage(), e);
          }
        } else {
          resMap.put(JsonKey.PROCESS_ID, resMap.get(JsonKey.ID));
          ProjectUtil.removeUnwantedFields(resMap, JsonKey.STATUS, JsonKey.ID);
          Map<String, Object> queryMap = new HashMap<>();
          queryMap.put(JsonKey.PROCESS_ID, processId);
          List<BulkUploadProcessTask> tasks = bulkUploadProcessTaskDao.readByPrimaryKeys(queryMap);

          List<Map> successList = new ArrayList<>();
          List<Map> failureList = new ArrayList<>();
          tasks
              .stream()
              .forEach(
                  x -> {
                    if (x.getStatus() == ProgressStatus.COMPLETED.getValue()) {
                      addTaskDataToList(successList, x.getData());
                    } else {
                      addTaskDataToList(failureList, x.getData());
                    }
                  });
          resMap.put(JsonKey.SUCCESS_RESULT, successList);
          resMap.put(JsonKey.FAILURE_RESULT, failureList);
        }
        sender().tell(response, self());
      } else {
        response = new Response();
        response.put(
            JsonKey.RESPONSE, "Operation is still in progress, Please try after some time.");
        sender().tell(response, self());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidProcessId.getErrorCode(),
          ResponseCode.invalidProcessId.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
  }

  private void addTaskDataToList(List<Map> list, String data) {
    try {
      list.add(mapper.readValue(data, Map.class));
    } catch (IOException ex) {
      ProjectLogger.log("Error while converting success list to map" + ex.getMessage(), ex);
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

  private void processBulkBatchEnrollment(Map<String, Object> req, String processId)
      throws IOException {
    List<String[]> batchList = parseCsvFile((byte[]) req.get(JsonKey.FILE), processId);
    if (null != batchList) {

      if (null != PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_BATCH_DATA_SIZE)) {
        batchDataSize =
            (Integer.parseInt(
                PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_BATCH_DATA_SIZE)));
        ProjectLogger.log("bulk upload batch data size read from config file " + batchDataSize);
      }
      validateFileSizeAgainstLineNumbers(batchDataSize, batchList.size());
      if (!batchList.isEmpty()) {
        String[] columns = batchList.get(0);
        validateBulkUploadFields(columns, bulkBatchAllowedFields, false);
      } else {
        throw new ProjectCommonException(
            ResponseCode.csvError.getErrorCode(),
            ResponseCode.csvError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.csvError.getErrorCode(),
          ResponseCode.csvError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    // save csv file to db
    uploadCsvToDB(
        batchList, processId, null, JsonKey.BATCH, (String) req.get(JsonKey.CREATED_BY), null);
  }

  private void processBulkOrgUpload(Map<String, Object> req, String processId) throws IOException {

    List<String[]> orgList = null;
    orgList = parseCsvFile((byte[]) req.get(JsonKey.FILE), processId);
    if (null != orgList) {
      if (null != PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)) {
        orgDataSize =
            (Integer.parseInt(
                PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_ORG_DATA_SIZE)));
        ProjectLogger.log("bulk upload org data size read from config file " + orgDataSize);
      }
      validateFileSizeAgainstLineNumbers(orgDataSize, orgList.size());
      if (!orgList.isEmpty()) {
        String[] columns = orgList.get(0);
        validateBulkUploadFields(columns, bulkOrgAllowedFields, false);
      } else {
        throw new ProjectCommonException(
            ResponseCode.dataSizeError.getErrorCode(),
            ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(), orgDataSize),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.dataSizeError.getErrorCode(),
          ProjectUtil.formatMessage(ResponseCode.dataSizeError.getErrorMessage(), orgDataSize),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    // save csv file to db
    uploadCsvToDB(
        orgList, processId, null, JsonKey.ORGANISATION, (String) req.get(JsonKey.CREATED_BY), null);
  }

  private void validateOrgProperty(String[] property) {
    ArrayList<String> properties =
        new ArrayList<>(
            Arrays.asList(
                JsonKey.ORGANISATION_NAME,
                JsonKey.CHANNEL,
                JsonKey.IS_ROOT_ORG,
                JsonKey.PROVIDER,
                JsonKey.EXTERNAL_ID,
                JsonKey.DESCRIPTION,
                JsonKey.HOME_URL,
                JsonKey.ORG_CODE,
                JsonKey.ORG_TYPE,
                JsonKey.PREFERRED_LANGUAGE,
                JsonKey.THEME,
                JsonKey.CONTACT_DETAILS,
                JsonKey.LOC_ID,
                JsonKey.HASHTAGID,
                JsonKey.LOCATION_CODE));

    for (String key : property) {
      if (!properties.contains(key)) {
        ProjectLogger.log("Invalid Column bulk upload management actor :: " + key);
        throw new ProjectCommonException(
            ResponseCode.InvalidColumnError.getErrorCode(),
            ResponseCode.InvalidColumnError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

  private void processBulkUserUpload(Map<String, Object> req, String processId) {
    String rootOrgId = "";
    try {
      // validate channel and set rootOrgId of user
      rootOrgId = Util.getRootOrgIdFromChannel((String) req.get(JsonKey.CHANNEL));
    } catch (Exception ex) {
      sender().tell(ex, self());
      return;
    }
    // --------------------------------------------------
    File file = new File("bulk.csv");
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      fos.write((byte[]) req.get(JsonKey.FILE));
    } catch (IOException e) {
      ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor", e);
    } finally {
      try {
        fos.close();
      } catch (Exception e) {
        ProjectLogger.log(
            "Exception Occurred while closing fileInputStream in BulkUploadManagementActor", e);
      }
    }
    List<String[]> userList = null;
    try {
      userList = parseCsvFile((byte[]) req.get(JsonKey.FILE), processId);
    } catch (IOException e) {
      throw new ProjectCommonException(
          ResponseCode.csvError.getErrorCode(),
          ResponseCode.csvError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (null != userList) {
      if (null != PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_USER_DATA_SIZE)) {
        userDataSize =
            (Integer.parseInt(
                PropertiesCache.getInstance().getProperty(JsonKey.BULK_UPLOAD_USER_DATA_SIZE)));

        ProjectLogger.log("bulk upload user data size read from config file " + userDataSize);
      }
      validateFileSizeAgainstLineNumbers(userDataSize, userList.size());
      if (!userList.isEmpty()) {
        String[] columns = userList.get(0);
        validateBulkUploadFields(columns, bulkUserAllowedFields, false);
      } else {
        throw new ProjectCommonException(
            ResponseCode.csvError.getErrorCode(),
            ResponseCode.csvError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.csvError.getErrorCode(),
          ResponseCode.csvError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    // save csv file to db
    uploadCsvToDB(
        userList,
        processId,
        (String) req.get(JsonKey.CHANNEL),
        JsonKey.USER,
        (String) req.get(JsonKey.CREATED_BY),
        rootOrgId);
  }

  private void uploadCsvToDB(
      List<String[]> dataList,
      String processId,
      String channel,
      String objectType,
      String requestedBy,
      String rootOrgId) {
    List<Map<String, Object>> dataMapList = new ArrayList<>();
    if (dataList.size() > 1) {
      try {
        String[] columnArr = dataList.get(0);
        columnArr = trimColumnAttributes(columnArr);
        Map<String, Object> dataMap = null;
        for (int i = 1; i < dataList.size(); i++) {
          dataMap = new HashMap<>();
          String[] valueArr = dataList.get(i);
          for (int j = 0; j < valueArr.length; j++) {
            String value = (valueArr[j].trim().length() == 0 ? null : valueArr[j].trim());
            dataMap.put(columnArr[j], value);
          }
          if (!StringUtils.isBlank(objectType) && objectType.equalsIgnoreCase(JsonKey.USER)) {
            dataMap.put(JsonKey.ROOT_ORG_ID, rootOrgId.trim());
            dataMap.put(JsonKey.CHANNEL, channel.trim());
          }
          dataMapList.add(dataMap);
        }
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
        throw new ProjectCommonException(
            ResponseCode.csvError.getErrorCode(),
            ResponseCode.csvError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      // tell sender that csv file is empty
      throw new ProjectCommonException(
          ResponseCode.csvError.getErrorCode(),
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
}
