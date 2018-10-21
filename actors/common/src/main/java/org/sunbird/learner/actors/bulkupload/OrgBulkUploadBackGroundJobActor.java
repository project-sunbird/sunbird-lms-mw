package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.org.OrganisationClient;
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.util.Util;

/** Created by rajatgupta on 15/10/18. */
@ActorConfig(
  tasks = {"orgBulkUploadBackground"},
  asyncTasks = {}
)
public class OrgBulkUploadBackGroundJobActor extends BaseBulkUploadBackGroundJobActor {
  private OrganisationClient orgClient = new OrganisationClientImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.ORGANISATION);
    ExecutionContext.setRequestId(request.getRequestId());
    if (operation.equalsIgnoreCase("orgBulkUploadBackground")) {
      handleBulkUploadBackround(
          request,
          (baseBulkUpload) -> {
            processBulkUpload(
                (BulkUploadProcess) baseBulkUpload,
                (task) -> {
                  processOrg((BulkUploadProcessTask) task);
                  return null;
                });
            return null;
          });
    } else {
      onReceiveUnsupportedOperation("OrgBulkUploadBackGroundJobActor");
    }
  }

  private void processOrg(BulkUploadProcessTask task) {
    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);
    String data = task.getData();
    try {
      Map<String, Object> row = mapper.readValue(data, Map.class);
      if (row.containsKey(JsonKey.ORG_ID) && row.get(JsonKey.ORG_ID) == null) {
        callCreateOrg(row, task);
      } else {
        callUpdateOrg(row, task);
      }
    } catch (IOException e) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }

  private void callCreateOrg(Map<String, Object> row, BulkUploadProcessTask task)
      throws JsonProcessingException {

    String orgId;
    try {
      orgId = orgClient.createOrg(getActorRef(ActorOperations.CREATE_ORG.getValue()), row);
    } catch (Exception ex) {
      ProjectLogger.log(
          "OrgBulkUploadBackGroundJobActor : callCreateLocation - got exception " + ex.getMessage(),
          LoggerEnum.INFO);
      setTaskStatus(
          task,
          ProjectUtil.BulkProcessStatus.FAILED.getValue(),
          ex.getMessage(),
          row,
          JsonKey.CREATE);
      return;
    }

    if (StringUtils.isEmpty(orgId)) {
      ProjectLogger.log(
          "OrgBulkUploadBackGroundJobActor : Null receive from interservice communication",
          LoggerEnum.ERROR);
      setTaskStatus(
          task,
          ProjectUtil.BulkProcessStatus.FAILED.getValue(),
          ResponseCode.internalError.getErrorMessage(),
          row,
          JsonKey.CREATE);
    } else {
      row.put(JsonKey.ID, orgId);
      setSuccessTaskStatus(
          task, ProjectUtil.ProgressStatus.COMPLETED.getValue(), row, JsonKey.CREATE);
    }
  }

  private void callUpdateOrg(Map<String, Object> row, BulkUploadProcessTask task)
      throws JsonProcessingException {
    try {
      orgClient.updateOrg(getActorRef(ActorOperations.UPDATE_ORG.getValue()), row);
    } catch (Exception ex) {
      ProjectLogger.log(
          "OrgBulkUploadBackGroundJobActor : callUpdateLocation - got exception " + ex.getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ex.getMessage());
      setTaskStatus(
          task,
          ProjectUtil.BulkProcessStatus.FAILED.getValue(),
          ex.getMessage(),
          row,
          JsonKey.UPDATE);
    }

    task.setData(mapper.writeValueAsString(row));
    setSuccessTaskStatus(
        task, ProjectUtil.ProgressStatus.COMPLETED.getValue(), row, JsonKey.UPDATE);
  }
}
