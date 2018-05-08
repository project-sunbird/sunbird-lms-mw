package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.core.service.InterServiceCommunication;
import org.sunbird.actor.core.service.InterServiceCommunicationFactory;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadJsonKey;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessTasksDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessTasksDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTasks;
import org.sunbird.learner.util.Util;

/** @author arvind */
@ActorConfig(
  tasks = {},
  asyncTasks = {"locationBulkUploadBackground"}
)
public class LocationBulkUploadBackGroundJobActor extends BaseActor {

  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();
  ObjectMapper mapper = new ObjectMapper();
  InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance().getCommunicationPath("actorCommunication");
  BulkUploadProcessTasksDao bulkUploadProcessTasksDao = new BulkUploadProcessTasksDaoImpl();

  @Override
  public void onReceive(Request request) throws Throwable {

    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    ExecutionContext.setRequestId(request.getRequestId());

    switch (operation) {
      case "locationBulkUploadBackground":
        bulkLocationUpload(request);
        break;
      default:
        ProjectLogger.log(operation + ": unsupported message");
    }
  }

  private void bulkLocationUpload(Request request) throws IOException {

    String processId = (String) request.get(JsonKey.PROCESS_ID);
    BulkUploadProcess bulkUploadProcess = bulkUploadDao.read(processId);
    if (null == bulkUploadProcess) {
      ProjectLogger.log("Process Id does not exist : " + processId, LoggerEnum.ERROR);
      return;
    }
    Integer status = bulkUploadProcess.getStatus();
    if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
      try {
        processLocationBulkUpoad(bulkUploadProcess);
      } catch (Exception ex) {
        bulkUploadProcess.setStatus(BulkProcessStatus.FAILED.getValue());
        bulkUploadProcess.setFailureResult(ex.getMessage());
        bulkUploadDao.update(bulkUploadProcess);
        ProjectLogger.log("Location Bulk BackGroungJob failed processId - " + processId, ex);
      }
    }
    bulkUploadProcess.setStatus(ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  private void processLocationBulkUpoad(BulkUploadProcess bulkUploadProcess)
      throws IOException, IllegalAccessException {

    Integer sequence = 0;
    Integer taskCount = bulkUploadProcess.getTaskCount();
    List<Map<String, Object>> successList = new LinkedList<>();
    List<Map<String, Object>> failureList = new LinkedList<>();
    while (sequence <= taskCount) {
      Integer nextSequence = sequence + 100;
      Map<String, Object> queryMap = new HashMap<>();
      queryMap.put(JsonKey.PROCESS_ID, bulkUploadProcess.getId());
      Map<String, Object> sequenceRange = new HashMap<>();
      sequenceRange.put(JsonKey.GT, sequence);
      sequenceRange.put(JsonKey.LTE, nextSequence);
      queryMap.put(BulkUploadJsonKey.SEQUENCE_ID, sequenceRange);
      List<BulkUploadProcessTasks> tasks = bulkUploadProcessTasksDao.readByPrimaryKeys(queryMap);
      for (BulkUploadProcessTasks task : tasks) {
        try {
          // since the same bblock of code will be use by the scheduler , so do not process those
          // records which are completed.
          if (task.getStatus() != null
              && task.getStatus() != ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
            processLocation(task);
            task.setUpdatedTs(new Timestamp(System.currentTimeMillis()));
            task.setIterationId(task.getIterationId() + 1);
          }
        } catch (Exception ex) {
          task.setFailureResult(ex.getMessage());
        }
      }
      bulkUploadProcessTasksDao.updateBatchRecord(tasks);
      sequence = nextSequence;
    }
    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocationBulkUpoad process finished",
        LoggerEnum.INFO);
    bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
    bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
    bulkUploadProcess.setStatus(BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  private void processLocation(BulkUploadProcessTasks task) throws IOException {

    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);

    String data = task.getData();
    Map<String, Object> row = mapper.readValue(data, Map.class);
    String locationType = (String) row.get(GeoLocationJsonKey.LOCATION_TYPE);

    if (checkMandatoryFields(row, GeoLocationJsonKey.CODE)) {
      Request request = new Request();
      Map<String, Object> filters = new HashMap<>();
      filters.put(GeoLocationJsonKey.CODE, row.get(GeoLocationJsonKey.CODE));
      filters.put(GeoLocationJsonKey.LOCATION_TYPE, locationType);
      request.getRequest().put(JsonKey.FILTERS, filters);

      Object obj =
          interServiceCommunication.getResponse(
              request, LocationActorOperation.SEARCH_LOCATION.getValue());
      if (null == obj) {
        ProjectLogger.log("Null receive from interservice communication", LoggerEnum.ERROR);
        setTaskStatus(
            task,
            BulkProcessStatus.FAILED.getValue(),
            "Null receive from interservice communication");
      } else if (obj instanceof ProjectCommonException) {
        setTaskStatus(
            task, BulkProcessStatus.FAILED.getValue(), ((ProjectCommonException) obj).getMessage());
      } else if (obj instanceof Response) {
        Response response = (Response) obj;
        List<Map<String, Object>> responseList =
            (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(responseList)) {
          callCreateLocation(row, task);
        } else {
          callUpdateLocation(row, responseList.get(0), task);
        }
      }
    } else {
      setTaskStatus(
          task,
          BulkProcessStatus.FAILED.getValue(),
          MessageFormat.format(
              ResponseCode.mandatoryParamsMissing.getErrorMessage(), GeoLocationJsonKey.CODE));
    }
  }

  private boolean checkMandatoryFields(Map<String, Object> row, String... fields) {

    boolean flag = true;
    for (String field : fields) {
      if (!(row.containsKey(field))) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  private void callUpdateLocation(
      Map<String, Object> row, Map<String, Object> response, BulkUploadProcessTasks task) {

    String id = (String) response.get(JsonKey.ID);
    row.put(JsonKey.ID, id);
    // since for update type is not allowed so remove from request body
    row.remove(GeoLocationJsonKey.LOCATION_TYPE);

    Request request = new Request();
    request.getRequest().putAll(row);
    ProjectLogger.log(
        "callUpdateLocation - "
            + (request instanceof Request)
            + "Operation -"
            + LocationActorOperation.UPDATE_LOCATION.getValue(),
        LoggerEnum.INFO);

    performLocationOperation(request, LocationActorOperation.UPDATE_LOCATION.getValue(), task);
  }

  private void callCreateLocation(Map<String, Object> row, BulkUploadProcessTasks task) {

    Request request = new Request();
    request.getRequest().putAll(row);
    ProjectLogger.log(
        "callCreateLocation - "
            + (request instanceof Request)
            + "Operation -"
            + LocationActorOperation.CREATE_LOCATION.getValue(),
        LoggerEnum.INFO);
    performLocationOperation(request, LocationActorOperation.CREATE_LOCATION.getValue(), task);
  }

  private void performLocationOperation(
      Request request, String operation, BulkUploadProcessTasks task) {
    Object obj = interServiceCommunication.getResponse(request, operation);
    if (null == obj) {
      ProjectLogger.log("Null receive from interservice communication", LoggerEnum.ERROR);
      setTaskStatus(
          task,
          BulkProcessStatus.FAILED.getValue(),
          "Null receive from interservice communication");
    } else if (obj instanceof ProjectCommonException) {
      ProjectLogger.log(
          "callUpdateLocation - got exception from UpdateLocationService "
              + ((ProjectCommonException) obj).getMessage(),
          LoggerEnum.INFO);
      setTaskStatus(
          task, BulkProcessStatus.FAILED.getValue(), ((ProjectCommonException) obj).getMessage());
    } else if (obj instanceof Response) {
      setTaskStatus(task, BulkProcessStatus.COMPLETED.getValue());
    }
  }

  private void setTaskStatus(BulkUploadProcessTasks task, Integer status, String failMessage) {
    if (BulkProcessStatus.COMPLETED.getValue() == status) {
      task.setStatus(status);
    } else if (BulkProcessStatus.FAILED.getValue() == status) {
      task.setFailureResult(failMessage);
      task.setStatus(status);
    }
  }

  private void setTaskStatus(BulkUploadProcessTasks task, Integer status) {
    task.setStatus(status);
  }

  private Map<String, Integer> getOrderMap() {
    Map<String, Integer> orderMap = new HashMap<>();
    List<String> subTypeList =
        Arrays.asList(
            ProjectUtil.getConfigValue(GeoLocationJsonKey.SUNBIRD_VALID_LOCATION_TYPES).split(";"));
    for (String str : subTypeList) {
      List<String> typeList =
          (((Arrays.asList(str.split(","))).stream().map(String::toLowerCase))
              .collect(Collectors.toList()));
      for (int i = 0; i < typeList.size(); i++) {
        orderMap.put(typeList.get(i), i);
      }
    }
    return orderMap;
  }
}
