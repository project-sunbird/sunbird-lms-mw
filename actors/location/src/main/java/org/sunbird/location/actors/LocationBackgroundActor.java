package org.sunbird.location.actors;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;

import java.util.Map;

/**
 * This class will handle all background service for locationActor.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {},
  asyncTasks = {"upsertLocationDataToES", "deleteLocationDataFromES"}
)
public class LocationBackgroundActor extends BaseLocationActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    ProjectLogger.log("LocationBackgroundActor onReceive called");
    String operation = request.getOperation();

    switch (operation) {
      case "upsertLocationDataToES":
        upsertLocationDataToES(request);
        break;
      case "deleteLocationDataFromES":
        deleteLocationDataFromES(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationBackgroundActor");
    }
  }

  private void deleteLocationDataFromES(Request request) {
    String locationId = (String) request.get(JsonKey.LOCATION_ID);
      EsClientFactory.getInstance(JsonKey.REST).delete(ProjectUtil.EsType.location.getTypeName(), locationId);
  }

  private void upsertLocationDataToES(Request request) {
    Map<String, Object> location = (Map<String, Object>) request.getRequest().get(JsonKey.LOCATION);
      EsClientFactory.getInstance(JsonKey.REST).upsert(
        ProjectUtil.EsType.location.getTypeName(), (String) location.get(JsonKey.ID), location);
  }
}
