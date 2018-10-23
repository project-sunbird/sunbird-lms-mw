package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
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

@ActorConfig(
  tasks = {},
  asyncTasks = {"orgBulkUploadBackground"}
)
public class OrgBulkUploadBackGroundJobActor extends BaseBulkUploadBackgroundJobActor {
  private OrganisationClient orgClient = new OrganisationClientImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.ORGANISATION);
    ExecutionContext.setRequestId(request.getRequestId());
    if (operation.equalsIgnoreCase("orgBulkUploadBackground")) {
      handleBulkUploadBackground(
          request,
          (baseBulkUpload) -> {
            processBulkUpload(
                (BulkUploadProcess) baseBulkUpload,
                (tasks) -> {
                  processTasks((List<BulkUploadProcessTask>) tasks);
                  return null;
                });
            return null;
          });
    } else {
      onReceiveUnsupportedOperation("OrgBulkUploadBackGroundJobActor");
    }
  }

  void processTasks(List<BulkUploadProcessTask> bulkUploadProcessTasks) {
    List<BulkUploadProcessTask> nonRootOrg = new ArrayList<>();
    for (BulkUploadProcessTask task : bulkUploadProcessTasks) {
      try {
        if (task.getStatus() != null
            && task.getStatus() != ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
          Map<String, Object> row = mapper.readValue(task.getData(), Map.class);
          if (row.containsKey(JsonKey.IS_ROOT_ORG)
              && row.get(JsonKey.IS_ROOT_ORG) != null
              && Boolean.valueOf((String) row.get(JsonKey.IS_ROOT_ORG))) {
            processOrg(task);
            task.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
            task.setIterationId(task.getIterationId() + 1);
          } else {
            nonRootOrg.add(task);
          }
        }
      } catch (Exception ex) {
        task.setFailureResult(ex.getMessage());
      }
    }
    if (!nonRootOrg.isEmpty()) {
      for (BulkUploadProcessTask task : nonRootOrg) {
        processOrg(task);
        task.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
        task.setIterationId(task.getIterationId() + 1);
      }
    }
  }

  private void processOrg(BulkUploadProcessTask task) {
    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);
    String data = task.getData();
    try {
      Map<String, Object> row = mapper.readValue(data, Map.class);
      if (StringUtils.isEmpty((String) row.get(JsonKey.ORG_ID))) {
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
