package org.sunbird.location.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * This class will handle all location related request.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {},
  asyncTasks = {}
)
public class LocationServiceActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    ProjectLogger.log("LocationServiceActor onReceive called");
    String operation = request.getOperation();

    switch (operation) {
      case "createLocation":
        createLocation(request);
        break;
      case "updateLocation":
        updateLocation(request);
        break;
      case "searchLocation":
        searchLocation(request);
        break;
      case "deleteLocation":
        deleteLocation(request);
        break;
      case "readLocationType":
        readLocationType(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationServiceActor");
    }
  }

  private void createLocation(Request request) {
    ProjectLogger.log("createLocation method called");
    List<Map<String, Object>> dataList =
        (List<Map<String, Object>>) request.getRequest().get("data");
    String objectType = (String) request.getRequest().get("objectType");
    List<Map<String, Object>> reponseList = new ArrayList<>();
    for (Map<String, Object> location : dataList) {
      reponseList.add(processLocationData(location, objectType));
      ProjectLogger.log("insert location data to ES");
      saveDataToES(location, JsonKey.INSERT);
    }
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, reponseList);
    sender().tell(response, self());
  }

  private void updateLocation(Request request) {
    ProjectLogger.log("updateLocation method called");
    String status = updateRecordToDb(request.getRequest());
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, status);
    sender().tell(response, self());
    ProjectLogger.log("update location data to ES");
    List<Map<String, Object>> reponseList = new ArrayList<>(1);
    reponseList.add(request.getRequest());
    saveDataToES(request.getRequest(), JsonKey.UPDATE);
  }

  private void searchLocation(Request request) {
    ProjectLogger.log("searchLocation method called");
    SearchDTO searchDto = Util.createSearchDto(request.getRequest());
    String[] types = {"location"};
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(), types);
    Response response = new Response();
    if (result != null) {
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());
  }

  private void deleteLocation(Request request) {
    ProjectLogger.log("deleteLocation method called");
    String status = deleteRecordFromDb((String) request.getRequest().get("locationId"));
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, status);
    sender().tell(response, self());
  }

  private void readLocationType(Request request) {
    ProjectLogger.log("readLocationType method called");
    sender().tell(cassandraOperation.getAllRecords("keyspaceName", "tableName"), self());
  }

  private Map<String, Object> processLocationData(Map<String, Object> location, String objectType) {
    ProjectLogger.log("processLocationData method called");
    String id = ProjectUtil.generateUniqueId();
    location.put(JsonKey.ID, id);
    location.put(JsonKey.OBJECT_TYPE, objectType);
    String status = insertRecordToDb(location);
    return generateResponse(location, status);
  }

  private Map<String, Object> generateResponse(Map<String, Object> location, String status) {
    ProjectLogger.log("generateResponse method called");
    // remove all unwanted data and add status
    location.remove("objectType");

    if (JsonKey.FAILURE.equalsIgnoreCase(status)) {
      location.remove(JsonKey.ID);
    }
    location.put(JsonKey.STATUS, status);
    return location;
  }

  private String insertRecordToDb(Map<String, Object> location) {
    ProjectLogger.log("insertRecordToDb method called");
    try {
      cassandraOperation.insertRecord("keyspaceName", "tableName", location);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      return JsonKey.FAILURE;
    }
    return JsonKey.SUCCESS;
  }

  private String updateRecordToDb(Map<String, Object> location) {
    ProjectLogger.log("updateRecordToDb method called");
    try {
      cassandraOperation.updateRecord("keyspaceName", "tableName", location);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      return JsonKey.FAILURE;
    }
    return JsonKey.SUCCESS;
  }

  private String deleteRecordFromDb(String locId) {
    ProjectLogger.log("deleteRecordFromDb method called");
    try {
      cassandraOperation.deleteRecord("keyspaceName", "tableName", locId);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      return JsonKey.FAILURE;
    }
    return JsonKey.SUCCESS;
  }

  private void saveDataToES(Map<String, Object> locData, String opType) {
    ProjectLogger.log("saveDataToES method called");
    Request request = new Request();
    request.setOperation("upsertLocationDataToES");
    request.getRequest().put(JsonKey.LOCATION, locData);
    request.getRequest().put(JsonKey.OPERATION_TYPE, opType);
    ProjectLogger.log("making a call to save location data to ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occured during saving location data to ES : ", ex);
    }
  }
}
