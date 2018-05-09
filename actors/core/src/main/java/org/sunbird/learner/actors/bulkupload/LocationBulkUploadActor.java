package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.sunbird.actor.core.bulkUpload.BaseBulkUploadActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessTasksDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessTasksDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTasks;
import org.sunbird.learner.util.Util;

/** @author arvind. */
@ActorConfig(
  tasks = {"locationBulkUpload"},
  asyncTasks = {}
)
public class LocationBulkUploadActor extends BaseBulkUploadActor {

  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();
  BulkUploadProcessTasksDao bulkUploadProcessTasksDao = new BulkUploadProcessTasksDaoImpl();
  String[] bulkLocationAllowedFields = {
    GeoLocationJsonKey.CODE,
    JsonKey.NAME,
    GeoLocationJsonKey.PARENT_CODE,
    GeoLocationJsonKey.PARENT_ID
  };
  ObjectMapper mapper = new ObjectMapper();
  private Integer CASSANDRA_WRITE_BATCH_SIZE = 100;

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    switch (operation) {
      case "locationBulkUpload":
        upload(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationBulkUploadActor");
    }
  }

  private void upload(Request request) throws IOException {

    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Response response = new Response();
    response.getResult().put(JsonKey.PROCESS_ID, processId);
    sender().tell(response, self());
    Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
    String locationType = (String) req.get(GeoLocationJsonKey.LOCATION_TYPE);
    processLocationBulkUpload(req, processId, locationType);
  }

  private void processLocationBulkUpload(
      Map<String, Object> req, String processId, String locationType) throws IOException {
    byte[] fileByteArray = null;
    if (null != req.get(JsonKey.FILE)) {
      fileByteArray = (byte[]) req.get(JsonKey.FILE);
    }
    Integer recordCount =
        validateAndParseRecords(fileByteArray, locationType, processId, bulkLocationAllowedFields);
    BulkUploadProcess bulkUploadProcess =
        getBulkUploadProcess(
            processId, JsonKey.LOCATION, (String) req.get(JsonKey.CREATED_BY), recordCount);

    Response res = bulkUploadDao.create(bulkUploadProcess);
    if (((String) res.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      Request request = new Request();
      request.put(JsonKey.PROCESS_ID, processId);
      request.setOperation(BulkUploadActorOperation.LOCATION_BULK_UPLOAD_BACKGROUND_JOB.getValue());
      ProjectLogger.log(
          "LocationBulkUploadActor : calling action"
              + BulkUploadActorOperation.LOCATION_BULK_UPLOAD_BACKGROUND_JOB.getValue());
      tellToAnother(request);
    }
  }

  private BulkUploadProcess getBulkUploadProcess(
      String processId, String objectType, String requestedBy, Integer taskCount) {
    BulkUploadProcess bulkUploadProcess = new BulkUploadProcess();
    bulkUploadProcess.setId(processId);
    bulkUploadProcess.setObjectType(objectType);
    bulkUploadProcess.setUploadedBy(requestedBy);
    bulkUploadProcess.setUploadedDate(ProjectUtil.getFormattedDate());
    bulkUploadProcess.setProcessStartTime(ProjectUtil.getFormattedDate());
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.NEW.getValue());
    bulkUploadProcess.setTaskCount(taskCount);
    return bulkUploadProcess;
  }

  private Integer validateAndParseRecords(
      byte[] fileByteArray, String locationType, String processId, String[] bulkUploadAllowedFields)
      throws IOException {

    Integer sequence = 0;
    Integer count = 0;
    CSVReader csvReader = null;
    String[] csvLine;
    String[] header = null;
    Map<String, Object> record = new HashMap<>();
    List<BulkUploadProcessTasks> records = new ArrayList<>();
    try {
      csvReader = getCsvReader(fileByteArray, ',', '"', 0);
      while ((csvLine = csvReader.readNext()) != null) {
        if (ProjectUtil.isNotEmptyStringArray(csvLine)) {
          continue;
        }
        if (sequence == 0) {
          sequence++;
          header = trimColumnAttributes(csvLine);
          validateBulkUploadFields(header, bulkUploadAllowedFields, true);
        } else {
          for (int j = 0; j < header.length; j++) {
            String value = (csvLine[j].trim().length() == 0 ? null : csvLine[j].trim());
            record.put(header[j], value);
          }
          record.put(GeoLocationJsonKey.LOCATION_TYPE, locationType);
          BulkUploadProcessTasks tasks = new BulkUploadProcessTasks();
          tasks.setStatus(ProjectUtil.BulkProcessStatus.NEW.getValue());
          tasks.setSequenceId(sequence);
          tasks.setProcessId(processId);
          tasks.setData(mapper.writeValueAsString(record));
          tasks.setCreatedTs(new Timestamp(System.currentTimeMillis()));
          records.add(tasks);
          sequence++;
          count++;
          if (count >= CASSANDRA_WRITE_BATCH_SIZE) {
            bulkUploadProcessTasksDao.insertBatchRecord(records);
            count = 0;
          }
          record.clear();
        }
      }
      if (count != 0) {
        bulkUploadProcessTasksDao.insertBatchRecord(records);
        count = 0;
      }
    } catch (Exception ex) {
      BulkUploadProcess bulkUploadProcess =
          getBulkUploadProcessForFailedStatus(processId, BulkProcessStatus.FAILED.getValue(), ex);
      bulkUploadDao.update(bulkUploadProcess);
      throw ex;
    } finally {
      IOUtils.closeQuietly(csvReader);
    }
    // since one record represents the header
    return sequence - 1;
  }
}
