package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;

@ActorConfig(
  tasks = {"orgBulkUpload"},
  asyncTasks = {}
)
public class OrgBulkUploadActor extends BaseBulkUploadActor {
  SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
  private int pos = 0;

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
    SystemSetting systemSetting =
        systemSettingClient.getSystemSettingByField(
            getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()), "orgProfileConfig");
    Map<String, Object> mandatoryMap = null;
    if (systemSetting != null) {
      ObjectMapper objectMapper = new ObjectMapper();
      Map<String, Object> valueMap = objectMapper.readValue(systemSetting.getValue(), Map.class);
      Map<String, Object> csvMap = objectMapper.convertValue(valueMap.get("csv"), Map.class);
      mandatoryMap = objectMapper.convertValue(csvMap.get("supportedColumns"), Map.class);
      String[] mandatoryFields = new String[mandatoryMap.size()];
      pos = 0;
      mandatoryMap.forEach((key, value) -> mandatoryFields[pos++] = key);
      validateFileHeaderFields(req, mandatoryFields, false);
    }
    BulkUploadProcess bulkUploadProcess =
        handleUpload(JsonKey.ORGANISATION, (String) req.get(JsonKey.CREATED_BY));
    processOrgBulkUpload(req, bulkUploadProcess.getId(), bulkUploadProcess, mandatoryMap);
  }

  private void processOrgBulkUpload(
      Map<String, Object> req,
      String processId,
      BulkUploadProcess bulkUploadProcess,
      Map<String, Object> mandatoryMap)
      throws IOException {
    byte[] fileByteArray = null;
    if (null != req.get(JsonKey.FILE)) {
      fileByteArray = (byte[]) req.get(JsonKey.FILE);
    }
    HashMap<String, Object> additionalInfo = new HashMap<>();
    Map<String, Object> user = getUser((String) req.get(JsonKey.CREATED_BY));
    if (user != null) {
      String rootOrgId = (String) user.get(JsonKey.ROOT_ORG_ID);
      Map<String, Object> org = getOrg(rootOrgId);
      if (org != null && (int) org.get(JsonKey.STATUS) == ProjectUtil.OrgStatus.ACTIVE.getValue()) {
        additionalInfo.put(JsonKey.CHANNEL, org.get(JsonKey.CHANNEL));
      }
    }
    if (!additionalInfo.containsKey(JsonKey.CHANNEL)) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.errorNoRootOrgAssociated,
          ResponseCode.errorNoRootOrgAssociated.getErrorMessage());
    }
    Integer recordCount =
        validateAndParseRecords(fileByteArray, processId, additionalInfo, mandatoryMap);
    processBulkUpload(
        recordCount,
        processId,
        bulkUploadProcess,
        BulkUploadActorOperation.ORG_BULK_UPLOAD_BACKGROUND_JOB.getValue());
  }

  Map<String, Object> getUser(String userId) {
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (result != null || result.size() > 0) {
      return result;
    }
    return null;
  }

  Map<String, Object> getOrg(String orgId) {
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            orgId);
    if (result != null || result.size() > 0) {
      return result;
    }
    return null;
  }
}
