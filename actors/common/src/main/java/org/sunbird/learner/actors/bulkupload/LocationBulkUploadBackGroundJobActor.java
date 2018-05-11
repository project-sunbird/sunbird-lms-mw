package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.location.LocationClient;
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.common.models.util.BulkUploadJsonKey;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessTaskDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessTaskDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.util.Util;
import org.sunbird.models.location.Location;

/**
 * @desc This class will do the bulk processing of Location.
 * @author Arvind
 */
@ActorConfig(
  tasks = {},
  asyncTasks = {"locationBulkUploadBackground"}
)
public class LocationBulkUploadBackGroundJobActor extends BaseBulkUploadActor {

  private LocationClient locationClient = new LocationClientImpl();
  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();
  ObjectMapper mapper = new ObjectMapper();
  InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();
  BulkUploadProcessTaskDao bulkUploadProcessTaskDao = new BulkUploadProcessTaskDaoImpl();

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
      Integer nextSequence = sequence + CASSANDRA_UPDATE_BATCH_SIZE;
      Map<String, Object> queryMap = new HashMap<>();
      queryMap.put(JsonKey.PROCESS_ID, bulkUploadProcess.getId());
      Map<String, Object> sequenceRange = new HashMap<>();
      sequenceRange.put(JsonKey.GT, sequence);
      sequenceRange.put(JsonKey.LTE, nextSequence);
      queryMap.put(BulkUploadJsonKey.SEQUENCE_ID, sequenceRange);
      List<BulkUploadProcessTask> tasks = bulkUploadProcessTaskDao.readByPrimaryKeys(queryMap);
      for (BulkUploadProcessTask task : tasks) {
        try {
          // since the same block of code will be use by the scheduler , so do not process those
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
      try {
        bulkUploadProcessTaskDao.updateBatchRecord(tasks);
      } catch (Exception ex) {
        for (BulkUploadProcessTask task : tasks) {
          try {
            bulkUploadProcessTaskDao.update(task);
          } catch (Exception exception) {
            ProjectLogger.log(
                "Cassandra Update failed for BulkUploadProcessTask-"
                    + task.getProcessId()
                    + task.getSequenceId(),
                exception);
          }
        }
      }
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

  private void processLocation(BulkUploadProcessTask task) throws IOException {

    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);
    String data = task.getData();
    Map<String, Object> row = mapper.readValue(data, Map.class);

    if (checkMandatoryFields(row, GeoLocationJsonKey.CODE)) {
      Location location = null;
      try {
        location =
            locationClient.getLocationByCode(
                getActorRef(LocationActorOperation.SEARCH_LOCATION.getValue()),
                (String) row.get(GeoLocationJsonKey.CODE));
      } catch (Exception ex) {
        row.put(JsonKey.ERROR_MSG, ex.getMessage());
        setTaskStatus(task, BulkProcessStatus.FAILED.getValue(), ex.getMessage());
      }
      if (null == location) {
        callCreateLocation(row, task);
      } else {
        callUpdateLocation(row, mapper.convertValue(location, Map.class), task);
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
      Map<String, Object> row, Map<String, Object> response, BulkUploadProcessTask task) {

    String id = (String) response.get(JsonKey.ID);
    row.put(JsonKey.ID, id);
    // since for update type is not allowed so remove from request body
    row.remove(GeoLocationJsonKey.LOCATION_TYPE);

    try {
      locationClient.updateLocation(
          getActorRef(LocationActorOperation.UPDATE_LOCATION.getValue()),
          mapper.convertValue(row, Location.class));
    } catch (Exception ex) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : callUpdateLocation - got exception "
              + ex.getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ex.getMessage());
      setTaskStatus(
          task,
          BulkProcessStatus.FAILED.getValue(),
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication");
    }
    setTaskStatus(task, ProgressStatus.COMPLETED.getValue());
  }

  private void callCreateLocation(Map<String, Object> row, BulkUploadProcessTask task)
      throws JsonProcessingException {

    Request request = new Request();
    request.getRequest().putAll(row);

    String locationId = "";
    try {
      locationId =
          locationClient.createLocation(
              getActorRef(LocationActorOperation.CREATE_LOCATION.getValue()),
              mapper.convertValue(row, Location.class));
    } catch (Exception ex) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : callCreateLocation - got exception "
              + ex.getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ex.getMessage());
      setTaskStatus(
          task,
          BulkProcessStatus.FAILED.getValue(),
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication");
    }

    if (StringUtils.isEmpty(locationId)) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication",
          LoggerEnum.ERROR);
      row.put(
          JsonKey.ERROR_MSG,
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication");
      setTaskStatus(
          task,
          BulkProcessStatus.FAILED.getValue(),
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication");
    } else {
      row.put(JsonKey.ID, locationId);
      task.setData(mapper.writeValueAsString(row));
      setTaskStatus(task, ProgressStatus.COMPLETED.getValue());
    }
  }

  private void setTaskStatus(BulkUploadProcessTask task, Integer status, String failMessage) {
    if (BulkProcessStatus.COMPLETED.getValue() == status) {
      task.setStatus(status);
    } else if (BulkProcessStatus.FAILED.getValue() == status) {
      task.setFailureResult(failMessage);
      task.setStatus(status);
    }
  }

  private void setTaskStatus(BulkUploadProcessTask task, Integer status) {
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
