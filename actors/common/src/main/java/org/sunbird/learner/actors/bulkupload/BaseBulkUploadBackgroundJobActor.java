package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.common.Constants;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.util.CloudUploadUtil;
import org.sunbird.common.util.CloudUploadUtil.CloudStorageTypes;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.actors.bulkupload.model.CloudStorageData;

public abstract class BaseBulkUploadBackgroundJobActor extends BaseBulkUploadActor {

  protected void setSuccessTaskStatus(
      BulkUploadProcessTask task,
      ProjectUtil.BulkProcessStatus status,
      Map<String, Object> row,
      String action)
      throws JsonProcessingException {
    row.put(JsonKey.OPERATION, action);
    task.setSuccessResult(mapper.writeValueAsString(row));
    task.setStatus(status.getValue());
  }

  protected void setTaskStatus(
      BulkUploadProcessTask task,
      ProjectUtil.BulkProcessStatus status,
      String failureMessage,
      Map<String, Object> row,
      String action)
      throws JsonProcessingException {
    row.put(JsonKey.OPERATION, action);
    if (ProjectUtil.BulkProcessStatus.COMPLETED.getValue() == status.getValue()) {
      task.setSuccessResult(mapper.writeValueAsString(row));
      task.setStatus(status.getValue());
    } else if (ProjectUtil.BulkProcessStatus.FAILED.getValue() == status.getValue()) {
      row.put(JsonKey.ERROR_MSG, failureMessage);
      task.setStatus(status.getValue());
      task.setFailureResult(mapper.writeValueAsString(row));
    }
  }

  public void handleBulkUploadBackground(Request request, Function function) {
    String processId = (String) request.get(JsonKey.PROCESS_ID);
    String logMessagePrefix =
        MessageFormat.format(
            "BaseBulkUploadBackGroundJobActor:handleBulkUploadBackground:{0}: ", processId);

    ProjectLogger.log(logMessagePrefix + "called", LoggerEnum.INFO);

    BulkUploadProcess bulkUploadProcess = bulkUploadDao.read(processId);
    if (null == bulkUploadProcess) {
      ProjectLogger.log(logMessagePrefix + "Invalid process ID.", LoggerEnum.ERROR);
      return;
    }

    int status = bulkUploadProcess.getStatus();
    if (!(ProjectUtil.BulkProcessStatus.COMPLETED.getValue() == status)
        || ProjectUtil.BulkProcessStatus.INTERRUPT.getValue() == status) {
      try {
        function.apply(bulkUploadProcess);
      } catch (Exception e) {
        bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.FAILED.getValue());
        bulkUploadProcess.setFailureResult(e.getMessage());
        bulkUploadDao.update(bulkUploadProcess);
        ProjectLogger.log(
            logMessagePrefix + "Exception occurred with error message = " + e.getMessage(), e);
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
      function.apply(tasks);

      try {
        for (BulkUploadProcessTask task : tasks) {

          if (task.getStatus().equals(ProjectUtil.BulkProcessStatus.FAILED.getValue())) {
            failureList.add(
                mapper.readValue(
                    task.getFailureResult(), new TypeReference<Map<String, Object>>() {}));
          } else if (task.getStatus().equals(ProjectUtil.BulkProcessStatus.COMPLETED.getValue())) {
            successList.add(
                mapper.readValue(
                    task.getSuccessResult(), new TypeReference<Map<String, Object>>() {}));
          }
        }

      } catch (IOException e) {
        ProjectLogger.log(
            "BaseBulkUploadBackgroundJobActor:processBulkUpload: exception." + e.getMessage(), e);
      }
      performBatchUpdate(tasks);
      sequence = nextSequence;
    }
    try {
      CloudStorageData cloudStorageData =
          uploadResultToCloud(bulkUploadProcess, successList, failureList);
      ProjectLogger.log(
          "BaseBulkUploadBackgroundJobActor:processBulkUpload: completed.", LoggerEnum.INFO);
      bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
      bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
      bulkUploadProcess.setCloudStorageDataWithPojo(cloudStorageData);
      bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    } catch (Exception e) {
      ProjectLogger.log(
          "BaseBulkUploadBackgroundJobActor:processBulkUpload: exception." + e.getMessage(), e);
    }
    bulkUploadDao.update(bulkUploadProcess);
  }
  
   protected void validateMandatoryFields(
      Map<String, Object> csvColumns, BulkUploadProcessTask task, String[] mandatoryFields)
      throws JsonProcessingException {
    if (mandatoryFields != null) {
      for (String field : mandatoryFields) {
        if (StringUtils.isEmpty((String) csvColumns.get(field))) {
          String errorMessage = MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), new Object[] {field});
          
          setTaskStatus(task, ProjectUtil.BulkProcessStatus.FAILED, errorMessage, csvColumns, JsonKey.CREATE);

          ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, errorMessage);
        }
      }
    }
  }

  private CloudStorageData uploadResultToCloud(
      BulkUploadProcess bulkUploadProcess,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList)
      throws IOException {

    String objKey = generateObjectKey(bulkUploadProcess);
    File file = getFileHandle(bulkUploadProcess.getObjectType());
    writeResultsToFile(file, successList, failureList);
    CloudUploadUtil.upload(
        CloudStorageTypes.AZURE, bulkUploadProcess.getObjectType(), objKey, file.getAbsolutePath());
    return new CloudStorageData(
        CloudStorageTypes.AZURE.getType(), bulkUploadProcess.getObjectType(), objKey);
  }

  private File getFileHandle(String objType) {

    File file = null;
    try {
      file = File.createTempFile(objType, "upload");
    } catch (IOException e) { // TODO Auto-generated catch block
      ProjectLogger.log(
          "BaseBulkUploadBackgroundJobActor:processBulkUpload: exception." + e.getMessage(), e);
    }
    return file;
  }

  private String generateObjectKey(BulkUploadProcess bulkUploadProcess) {
    String objType = bulkUploadProcess.getObjectType();
    String processId = bulkUploadProcess.getId();
    return objType + "_" + processId + ".csv";
  }

  private void writeResultsToFile(
      File file, List<Map<String, Object>> successList, List<Map<String, Object>> failureList)
      throws IOException {
    try (CSVWriter csvWriter = new CSVWriter(new FileWriter(file)); ) {
      List<String> headerRows = getHeaderRow(successList, failureList);
      headerRows.add(JsonKey.BULK_UPLOAD_STATUS);
      headerRows.add(JsonKey.BULK_UPLOAD_ERROR);
      csvWriter.writeNext(headerRows.toArray(new String[0]));
      addResults(successList, headerRows, csvWriter);
      addResults(failureList, headerRows, csvWriter);
      csvWriter.flush();
    }
  }

  private void addResults(
      List<Map<String, Object>> resultList, List<String> headerRows, CSVWriter csvWriter) {
    resultList
        .stream()
        .forEach(
            map -> {
              String[] nextLine = new String[headerRows.size()];
              String errMsg = (String) map.get(JsonKey.ERROR_MSG);
              int i = 0;
              for (String field : headerRows) {

                if (JsonKey.BULK_UPLOAD_STATUS.equals(field)) {
                  nextLine[i++] = errMsg == null ? JsonKey.SUCCESS : JsonKey.FAILED;
                } else if (JsonKey.BULK_UPLOAD_ERROR.equals(field)) {
                  nextLine[i++] = errMsg == null ? "" : errMsg;
                } else {
                  nextLine[i++] = String.valueOf(map.get(field));
                }
              }
              csvWriter.writeNext(nextLine);
            });
  }

  private List<String> getHeaderRow(
      List<Map<String, Object>> successList, List<Map<String, Object>> failureList) {
    if (CollectionUtils.isNotEmpty(successList)) {
      Map<String, Object> firstRow = successList.get(0);
      return firstRow.keySet().stream().collect(Collectors.toList());
    }
    if (CollectionUtils.isNotEmpty(failureList)) {
      Map<String, Object> firstRow = failureList.get(0);
      return firstRow
          .keySet()
          .stream()
          .filter(s -> !JsonKey.ERROR_MSG.equals(s))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }
}
