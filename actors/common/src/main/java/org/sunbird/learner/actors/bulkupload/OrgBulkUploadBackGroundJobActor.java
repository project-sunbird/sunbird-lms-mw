package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.org.OrganisationClient;
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.common.Constants;
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

  @Override
  public void processBulkUpload(BulkUploadProcess bulkUploadProcess, Function function) {
    Integer sequence = 0;
    Integer taskCount = bulkUploadProcess.getTaskCount();
    List<Map<String, Object>> successList = new LinkedList<>();
    List<Map<String, Object>> failureList = new LinkedList<>();
    List<BulkUploadProcessTask> nonRootOrg = new ArrayList<>();
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
      performBatchUpdate(tasks);
      sequence = nextSequence;
    }
    ProjectLogger.log(
        "OrgBulkUploadBackGroundJobActor : processBulkUpload process finished", LoggerEnum.INFO);
    bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
    bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
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
