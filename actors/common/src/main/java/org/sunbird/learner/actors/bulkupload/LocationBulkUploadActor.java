package org.sunbird.learner.actors.bulkupload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;

/**
 * Class to provide the location bulk upload functionality.
 *
 * @author arvind.
 */
@ActorConfig(
  tasks = {"locationBulkUpload"},
  asyncTasks = {}
)
public class LocationBulkUploadActor extends BaseBulkUploadActor {

  String[] bulkLocationAllowedFields = {
    GeoLocationJsonKey.CODE,
    JsonKey.NAME,
    GeoLocationJsonKey.PARENT_CODE,
    GeoLocationJsonKey.PARENT_ID
  };
  private Integer DEFAULT_WRITE_BATCH_SIZE = 10;
  private Integer CASSANDRA_WRITE_BATCH_SIZE = getBatchSize(JsonKey.CASSANDRA_WRITE_BATCH_SIZE);

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
    Map<String, Object> additionalRowFields = new HashMap<>();
    additionalRowFields.put(GeoLocationJsonKey.LOCATION_TYPE, locationType);
    Integer recordCount =
        validateAndParseRecords(
            fileByteArray, processId, bulkLocationAllowedFields, additionalRowFields);
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
}
