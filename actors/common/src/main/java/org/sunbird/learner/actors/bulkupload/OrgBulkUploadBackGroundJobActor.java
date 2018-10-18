package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.org.OrganisationClient;
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.common.Constants;
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
public class OrgBulkUploadBackGroundJobActor extends BaseBulkUploadActor {
  OrganisationClient orgClient = new OrganisationClientImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.ORGANISATION);
    ExecutionContext.setRequestId(request.getRequestId());

    switch (operation) {
      case "orgBulkUploadBackground":
        bulkOrgUpload(request);
        break;
      default:
        ProjectLogger.log(operation + ": unsupported message");
    }
  }

  private void bulkOrgUpload(Request request) {
    String processId = (String) request.get(JsonKey.PROCESS_ID);
    ProjectLogger.log(
        "OrgBulkUploadBackGroundJobActor: bulkOrgUpload called with process ID " + processId,
        LoggerEnum.INFO);
    BulkUploadProcess bulkUploadProcess = bulkUploadDao.read(processId);
    if (null == bulkUploadProcess) {
      ProjectLogger.log("Process Id does not exist : " + processId, LoggerEnum.ERROR);
      return;
    }
    Integer status = bulkUploadProcess.getStatus();
    if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
      try {
        processOrgBulkUpload(bulkUploadProcess);
      } catch (Exception ex) {
        bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.FAILED.getValue());
        bulkUploadProcess.setFailureResult(ex.getMessage());
        bulkUploadDao.update(bulkUploadProcess);
        ProjectLogger.log("Org Bulk BackGroundJob failed processId - " + processId, ex);
      }
    }
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  private void processOrgBulkUpload(BulkUploadProcess bulkUploadProcess)
      throws IOException, IllegalAccessException {

    Integer sequence = 0;
    Integer taskCount = bulkUploadProcess.getTaskCount();
    List<Map<String, Object>> successList = new LinkedList<>();
    List<Map<String, Object>> failureList = new LinkedList<>();
    while (sequence <= taskCount) {
      Integer nextSequence = sequence + CASSANDRA_BATCH_SIZE;
      Map<String, Object> queryMap = new HashMap<>();
      queryMap.put(JsonKey.PROCESS_ID, bulkUploadProcess.getId());
      Map<String, Object> sequenceRange = new HashMap<>();
      sequenceRange.put(Constants.GT, sequence);
      sequenceRange.put(Constants.LTE, nextSequence);
      queryMap.put(BulkUploadJsonKey.SEQUENCE_ID, sequenceRange);
      List<BulkUploadProcessTask> tasks = bulkUploadProcessTaskDao.readByPrimaryKeys(queryMap);
      for (BulkUploadProcessTask task : tasks) {
        try {
          if (task.getStatus() != null
              && task.getStatus() != ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
            processOrg(task);
            task.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
            task.setIterationId(task.getIterationId() + 1);
          }
        } catch (Exception ex) {
          task.setFailureResult(ex.getMessage());
        }
      }
      performBatchUpdate(tasks);
      sequence = nextSequence;
    }
    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocationBulkUpoad process finished",
        LoggerEnum.INFO);
    bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
    bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  private void processOrg(BulkUploadProcessTask task) throws IOException {
    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);
    String data = task.getData();
    Map<String, Object> row = mapper.readValue(data, Map.class);
    if (row.containsKey(JsonKey.ORG_ID) && row.get(JsonKey.ORG_ID) == null) {
      callCreateOrg(row, task);
    } else {
      callUpdateOrg(row, task);
    }
  }

  private void callCreateOrg(Map<String, Object> row, BulkUploadProcessTask task)
      throws JsonProcessingException {
    Request request = new Request();
    request.getRequest().putAll(row);

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

  private void setSuccessTaskStatus(
      BulkUploadProcessTask task, Integer status, Map<String, Object> row, String action)
      throws JsonProcessingException {
    row.put(JsonKey.OPERATION, action);
    task.setSuccessResult(mapper.writeValueAsString(row));
    task.setStatus(ProjectUtil.ProgressStatus.COMPLETED.getValue());
  }

  private void setTaskStatus(
      BulkUploadProcessTask task,
      Integer status,
      String failureMessage,
      Map<String, Object> row,
      String action)
      throws JsonProcessingException {
    row.put(JsonKey.OPERATION, action);
    if (ProjectUtil.BulkProcessStatus.COMPLETED.getValue() == status) {
      task.setSuccessResult(mapper.writeValueAsString(row));
      task.setStatus(status);
    } else if (ProjectUtil.BulkProcessStatus.FAILED.getValue() == status) {
      row.put(JsonKey.ERROR_MSG, failureMessage);
      task.setStatus(status);
      task.setFailureResult(mapper.writeValueAsString(row));
    }
  }
}
