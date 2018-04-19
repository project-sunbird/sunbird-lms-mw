package org.sunbird.location.actors;

import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;

/**
 * This class will handle all background service for locationServiceActor.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {},
  asyncTasks = {"upsertLocationDataToES"}
)
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
      ElasticSearchUtil.createData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.location.getTypeName(),
          (String) location.get(JsonKey.ID),
          location);
    } else if (JsonKey.UPDATE.equalsIgnoreCase(opType)) {
      ElasticSearchUtil.updateData(
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.location.getTypeName(),
          (String) location.get(JsonKey.ID),
          location);
    }
  }
}
