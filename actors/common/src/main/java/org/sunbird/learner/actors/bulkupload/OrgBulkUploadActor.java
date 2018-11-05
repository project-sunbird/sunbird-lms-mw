package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

@ActorConfig(
  tasks = {"orgBulkUpload"},
  asyncTasks = {}
)
public class OrgBulkUploadActor extends BaseBulkUploadActor {
  private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
  private String[] bulkOrgAllowedFields = {
    JsonKey.ORGANISATION_ID,
    JsonKey.ORGANISATION_NAME,
    JsonKey.EXTERNAL_ID,
    JsonKey.DESCRIPTION,
    JsonKey.LOCATION_CODE,
    JsonKey.STATUS,
    JsonKey.CHANNEL,
    JsonKey.IS_ROOT_ORG,
    JsonKey.PROVIDER,
    JsonKey.HOME_URL,
    JsonKey.ORG_CODE,
    JsonKey.ORG_TYPE,
    JsonKey.PREFERRED_LANGUAGE,
    JsonKey.THEME,
    JsonKey.CONTACT_DETAILS,
    JsonKey.LOC_ID,
    JsonKey.HASHTAGID,
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
    Object dataObject =
        systemSettingClient.getSystemSettingByFieldKey(
            getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
            "orgProfileConfig",
            "csv.supportedColumns",
            new TypeReference<Map>() {});
    Map<String, Object> mandatoryMap = null;
    if (dataObject != null) {
      mandatoryMap = (Map<String, Object>) dataObject;
      List<String> madatoryList = new ArrayList<>();
      mandatoryMap.forEach((key, value) -> madatoryList.add(key));
      validateFileHeaderFields(req, madatoryList.toArray(new String[madatoryList.size()]), false);
    } else {
      validateFileHeaderFields(req, bulkOrgAllowedFields, false);
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
