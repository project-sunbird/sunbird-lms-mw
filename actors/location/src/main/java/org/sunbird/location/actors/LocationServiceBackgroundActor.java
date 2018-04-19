package org.sunbird.location.actors;

import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

/**
 * This class will handle all background service for locationServiceActor.
 *
 * @author Amit Kumar
 */
public class LocationServiceBackgroundActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    ProjectLogger.log("LocationServiceBackgroundActor onReceive called");
    String operation = request.getOperation();

    switch (operation) {
      case "upsertLocationDataToES":
        upsertLocationDataToES(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationServiceBackgroundActor");
    }
  }

  private void upsertLocationDataToES(Request request) {
    Map<String, Object> location = (Map<String, Object>) request.getRequest().get(JsonKey.LOCATION);
    String opType = (String) request.getRequest().get(JsonKey.OPERATION_TYPE);
    if (JsonKey.INSERT.equalsIgnoreCase(opType)) {
      ElasticSearchUtil.createData("index", "type", "identifier", location);
    } else if (JsonKey.UPDATE.equalsIgnoreCase(opType)) {
      ElasticSearchUtil.updateData("index", "type", "identifier", location);
    }
  }
}
