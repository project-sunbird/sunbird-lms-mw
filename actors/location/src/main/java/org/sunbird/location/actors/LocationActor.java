package org.sunbird.location.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.dao.impl.LocationDaoImpl;
import org.sunbird.location.model.Location;

/**
 * This class will handle all location related request.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {
    "createLocation",
    "updateLocation",
    "searchLocation",
    "deleteLocation",
    "readLocationType"
  },
  asyncTasks = {}
)
public class LocationActor extends BaseActor {

  private ObjectMapper mapper = new ObjectMapper();
  private LocationDao locationDao = new LocationDaoImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    ProjectLogger.log("LocationActor onReceive called");
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
        readLocationType();
        break;
      default:
        onReceiveUnsupportedOperation("LocationActor");
    }
  }

  private void createLocation(Request request) {
    ProjectLogger.log("createLocation method called");
    try {
      Map<String, Object> data = ((Map<String, Object>) request.getRequest().get(JsonKey.DATA));
      // put unique identifier in request for Id
      data.put(JsonKey.ID, ProjectUtil.generateUniqueId());
      Location location = mapper.convertValue(data, Location.class);
      Response response = locationDao.create(location);
      sender().tell(response, self());
      ProjectLogger.log("insert location data to ES");
      saveDataToES(data, JsonKey.INSERT);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      sender().tell(ex, self());
    }
  }

  private void updateLocation(Request request) {
    ProjectLogger.log("updateLocation method called");
    try {
      Map<String, Object> data = ((Map<String, Object>) request.getRequest().get(JsonKey.DATA));
      Response response = locationDao.update(mapper.convertValue(data, Location.class));
      sender().tell(response, self());
      ProjectLogger.log("update location data to ES");
      saveDataToES(data, JsonKey.UPDATE);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      sender().tell(ex, self());
    }
  }

  private void searchLocation(Request request) {
    ProjectLogger.log("searchLocation method called");
    try {
      Response response = locationDao.search(request.getRequest());
      sender().tell(response, self());
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      sender().tell(ex, self());
    }
  }

  private void deleteLocation(Request request) {
    ProjectLogger.log("deleteLocation method called");
    try {
      String locationId = (String) request.getRequest().get(JsonKey.LOCATION_ID);
      Response response = locationDao.delete(locationId);
      sender().tell(response, self());
      ProjectLogger.log("delete location data from ES");
      deleteDataFromES(locationId);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      sender().tell(ex, self());
    }
  }

  private void readLocationType() {
    ProjectLogger.log("readLocationType method called");
    try {
      Response response = locationDao.readAll();
      sender().tell(response, self());
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      sender().tell(ex, self());
    }
  }

  private void saveDataToES(Map<String, Object> locData, String opType) {
    ProjectLogger.log("saveDataToES method called");
    Request request = new Request();
    request.setOperation(LocationActorOperation.UPSERT_LOCATION_TO_ES.getValue());
    request.getRequest().put(JsonKey.LOCATION, locData);
    request.getRequest().put(JsonKey.OPERATION_TYPE, opType);
    ProjectLogger.log("making a call to save location data to ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occured during saving location data to ES : ", ex);
    }
  }

  private void deleteDataFromES(String locId) {
    ProjectLogger.log("saveDataToES method called");
    Request request = new Request();
    request.setOperation(LocationActorOperation.DELETE_LOCATION_FROM_ES.getValue());
    request.getRequest().put(JsonKey.LOCATION_ID, locId);
    ProjectLogger.log("making a call to delete location data from ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occured during saving location data to ES : ", ex);
    }
  }
}
