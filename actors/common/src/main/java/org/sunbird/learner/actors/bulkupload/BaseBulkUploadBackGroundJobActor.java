package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import org.sunbird.common.Constants;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;

public abstract class BaseBulkUploadBackGroundJobActor extends BaseBulkUploadActor {

  public void setSuccessTaskStatus(
      BulkUploadProcessTask task, Integer status, Map<String, Object> row, String action)
      throws JsonProcessingException {
    row.put(JsonKey.OPERATION, action);
    task.setSuccessResult(mapper.writeValueAsString(row));
    task.setStatus(ProjectUtil.ProgressStatus.COMPLETED.getValue());
  }

  public void setTaskStatus(
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

  public void handleBulkUploadBackround(Request request, Function function) {
    String processId = (String) request.get(JsonKey.PROCESS_ID);
    String logMessagePrefix = MessageFormat("BaseBulkUploadBackGroundJobActor:handleBulkUploadBackround:{0}: ", processId);
    
    ProjectLogger.log(logMessagePrefix + "called", LoggerEnum.INFO);
    
    BulkUploadProcess bulkUploadProcess = bulkUploadDao.read(processId);
    if (null == bulkUploadProcess) {
      ProjectLogger.log(logMessagePrefix + "Invalid process ID.", LoggerEnum.ERROR);
      return;
    }

    Integer status = bulkUploadProcess.getStatus();
    if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
      try {
        function.apply(bulkUploadProcess);
      } catch (Exception e) {
        bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.FAILED.getValue());
        bulkUploadProcess.setFailureResult(ex.getMessage());
        bulkUploadDao.update(bulkUploadProcess);
        ProjectLogger.log(logMessagePrefix + "Exception occurred with error message = " + e.getMessage(), e);
      }
    }
    
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  public void processBulkUpload(BulkUploadProcess bulkUploadProcess, Function function) {
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
            function.apply(task);
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
    
    ProjectLogger.log("BaseBulkUploadBackgroundJobActor:processBulkUpload: completed.", LoggerEnum.INFO);
    
    bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
    bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadProcess.setLastupdatedOn(new Timestamp(Calendar.getInstance().getTime().getTime()));
    bulkUploadDao.update(bulkUploadProcess);
  }
}
