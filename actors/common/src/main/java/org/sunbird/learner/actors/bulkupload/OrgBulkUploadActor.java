package org.sunbird.learner.actors.bulkupload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;

/** Created by rajatgupta on 15/10/18. */
@ActorConfig(
  tasks = {"orgBulkUpload"},
  asyncTasks = {}
)
public class OrgBulkUploadActor extends BaseBulkUploadActor {

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
    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    switch (operation) {
      case "orgBulkUpload":
        upload(request);
        break;
      default:
        onReceiveUnsupportedOperation("OrgBulkUploadActor");
    }
  }

  private void upload(Request request) throws IOException {
    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Response response = new Response();
    response.getResult().put(JsonKey.PROCESS_ID, processId);
    Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
    validateFileHeaderFields(req, bulkOrgAllowedFields, true);
    BulkUploadProcess bulkUploadProcess =
        getBulkUploadProcess(
            processId, JsonKey.ORGANISATION, (String) req.get(JsonKey.CREATED_BY), 0);
    Response res = bulkUploadDao.create(bulkUploadProcess);
    if (((String) res.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      sender().tell(response, self());
    } else {
      ProjectLogger.log("Exception occurred while inserting record in bulk_upload_process.");
      throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }

    processOrgBulkUpload(req, processId, bulkUploadProcess);
  }

  private void processOrgBulkUpload(
      Map<String, Object> req, String processId, BulkUploadProcess bulkUploadProcess)
      throws IOException {
    byte[] fileByteArray = null;
    if (null != req.get(JsonKey.FILE)) {
      fileByteArray = (byte[]) req.get(JsonKey.FILE);
    }
    Integer recordCount = validateAndParseRecords(fileByteArray, processId, new HashMap<>());

    // Update process ID in DB with actual task / record count
    bulkUploadProcess.setTaskCount(recordCount);
    bulkUploadDao.update(bulkUploadProcess);

    Request request = new Request();
    request.put(JsonKey.PROCESS_ID, processId);
    request.setOperation(BulkUploadActorOperation.ORG_BULK_UPLOAD_BACKGROUND_JOB.getValue());
    ProjectLogger.log(
        "LocationBulkUploadActor : calling action"
            + BulkUploadActorOperation.ORG_BULK_UPLOAD_BACKGROUND_JOB.getValue());
    tellToAnother(request);
  }
}
