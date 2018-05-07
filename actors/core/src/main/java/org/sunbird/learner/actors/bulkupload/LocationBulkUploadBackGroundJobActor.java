package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.location.LocationClient;
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
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
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadProcessDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.util.Util;

/** Created by arvind on 24/4/18. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"locationBulkUploadBackground"}
)
public class LocationBulkUploadBackGroundJobActor extends BaseActor {

  BulkUploadProcessDao bulkUploadDao = new BulkUploadProcessDaoImpl();
  ObjectMapper mapper = new ObjectMapper();
  private LocationClient locationClient = new LocationClientImpl();

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
        onReceiveUnsupportedOperation("LocationBulkUploadBackGroundJobActor");
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
      processLocationBulkUpoad(bulkUploadProcess);
    }
  }

  private void processLocationBulkUpoad(BulkUploadProcess bulkUploadProcess) throws IOException {

    TypeReference<List<Map<String, Object>>> mapType =
        new TypeReference<List<Map<String, Object>>>() {};
    List<Map<String, Object>> jsonList = new LinkedList<>();
    List<Map<String, Object>> successList = new LinkedList<>();
    List<Map<String, Object>> failureList = new LinkedList<>();
    try {
      jsonList = mapper.readValue(bulkUploadProcess.getData(), mapType);
    } catch (Exception e) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : Exception occurred while converting json String to List:",
          e);
      throw e;
    }

    for (Map<String, Object> row : jsonList) {
      processLocation(row, successList, failureList);
    }

    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocationBulkUpoad process finished",
        LoggerEnum.INFO);
    bulkUploadProcess.setSuccessResult(ProjectUtil.convertMapToJsonString(successList));
    bulkUploadProcess.setFailureResult(ProjectUtil.convertMapToJsonString(failureList));
    bulkUploadProcess.setStatus(BulkProcessStatus.COMPLETED.getValue());
    bulkUploadDao.update(bulkUploadProcess);
  }

  private void processLocation(
      Map<String, Object> row,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList) {

    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor : processLocation method called", LoggerEnum.INFO);

    if (checkMandatoryFields(row, GeoLocationJsonKey.CODE)) {
      Object obj =
          locationClient.getLocationByCode(
              getActorRef(LocationActorOperation.SEARCH_LOCATION.getValue()),
              (String) row.get(GeoLocationJsonKey.CODE));
      if (null == obj) {
        ProjectLogger.log(
            "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication",
            LoggerEnum.ERROR);
        failureList.add(row);
      } else if (obj instanceof ProjectCommonException) {
        row.put(JsonKey.ERROR_MSG, ((ProjectCommonException) obj).getMessage());
        failureList.add(row);
      } else if (obj instanceof Response) {
        Response response = (Response) obj;
        List<Map<String, Object>> responseList =
            (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(responseList)) {
          callCreateLocation(row, successList, failureList);
        } else {
          callUpdateLocation(row, successList, failureList, responseList.get(0));
        }
      }
    } else {
      row.put(
          JsonKey.ERROR_MSG,
          MessageFormat.format(
              ResponseCode.mandatoryParamsMissing.getErrorMessage(), GeoLocationJsonKey.CODE));
      failureList.add(row);
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
      Map<String, Object> row,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList,
      Map<String, Object> response) {

    String id = (String) response.get(JsonKey.ID);
    row.put(JsonKey.ID, id);
    Object obj =
        locationClient.updateLocation(
            getActorRef(LocationActorOperation.UPDATE_LOCATION.getValue()), row);
    if (null == obj) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication",
          LoggerEnum.ERROR);
      failureList.add(row);
    } else if (obj instanceof ProjectCommonException) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : callUpdateLocation - got exception from UpdateLocationService "
              + ((ProjectCommonException) obj).getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ((ProjectCommonException) obj).getMessage());
      failureList.add(row);
    } else if (obj instanceof Response) {
      successList.add(row);
    }
  }

  private void callCreateLocation(
      Map<String, Object> row,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList) {

    Object obj =
        locationClient.updateLocation(
            getActorRef(LocationActorOperation.CREATE_LOCATION.getValue()), row);

    if (null == obj) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : Null receive from interservice communication",
          LoggerEnum.ERROR);
      failureList.add(row);
    } else if (obj instanceof ProjectCommonException) {
      ProjectLogger.log(
          "LocationBulkUploadBackGroundJobActor : callCreateLocation - got exception from CreateLocationService "
              + ((ProjectCommonException) obj).getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ((ProjectCommonException) obj).getMessage());
      failureList.add(row);
    } else if (obj instanceof Response) {
      successList.add(row);
    }
  }
}
