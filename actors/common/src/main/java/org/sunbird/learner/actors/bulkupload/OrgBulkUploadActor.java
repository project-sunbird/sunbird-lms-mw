package org.sunbird.learner.actors.bulkupload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;

@ActorConfig(
  tasks = {"orgBulkUpload"},
  asyncTasks = {}
)
public class OrgBulkUploadActor extends BaseBulkUploadActor {

  private String[] bulkOrgAllowedFields = {
    JsonKey.ORG_ID,
    JsonKey.ORGANISATION_NAME,
    JsonKey.EXTERNAL_ID,
    JsonKey.DESCRIPTION,
    JsonKey.LOCATION_CODE,
    JsonKey.STATUS
  };

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.ORGANISATION);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();
    if (operation.equalsIgnoreCase("orgBulkUpload")) {
      upload(request);
    } else {
      onReceiveUnsupportedOperation("OrgBulkUploadActor");
    }
  }

  private void upload(Request request) throws IOException {
    Map<String, Object> req = (Map<String, Object>) request.getRequest().get(JsonKey.DATA);
    validateFileHeaderFields(req, bulkOrgAllowedFields, true);
    BulkUploadProcess bulkUploadProcess =
        handleUpload(JsonKey.ORGANISATION, (String) req.get(JsonKey.CREATED_BY));
    processOrgBulkUpload(req, bulkUploadProcess.getId(), bulkUploadProcess);
  }

  private void processOrgBulkUpload(
      Map<String, Object> req, String processId, BulkUploadProcess bulkUploadProcess)
      throws IOException {
    byte[] fileByteArray = null;
    if (null != req.get(JsonKey.FILE)) {
      fileByteArray = (byte[]) req.get(JsonKey.FILE);
    }
    HashMap<String, Object> additionalInfo = new HashMap<>();
    Map<String, Object> user = Util.getUserbyUserId((String) req.get(JsonKey.CREATED_BY));
    if (user != null) {
      additionalInfo.put(JsonKey.CHANNEL, user.get(JsonKey.CHANNEL));
    }
    Integer recordCount = validateAndParseRecords(fileByteArray, processId, additionalInfo);
    processBulkUpload(
        recordCount,
        processId,
        bulkUploadProcess,
        BulkUploadActorOperation.ORG_BULK_UPLOAD_BACKGROUND_JOB.getValue());
  }
}
