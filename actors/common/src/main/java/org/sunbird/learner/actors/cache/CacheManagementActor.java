package org.sunbird.learner.actors.cache;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cache.CacheFactory;
import org.sunbird.cache.interfaces.Cache;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

@ActorConfig(
  tasks = {"clearCache"},
  asyncTasks = {}
)
public class CacheManagementActor extends BaseActor {

  private Cache cache = CacheFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equalsIgnoreCase(ActorOperations.CLEAR_CACHE.getValue())) {
      clearCache(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void clearCache(Request request) {
    String mapName = (String) request.getContext().get(JsonKey.MAP_NAME);

    Response response = new Response();
    if (!JsonKey.ALL.equals(mapName)) {
      try {
        cache.clear(mapName);
        response.setResponseCode(ResponseCode.success);
        ProjectLogger.log("CacheManagementActor:clearCache :mapName : " + mapName, LoggerEnum.INFO);
        sender().tell(response, self());
      } catch (Exception e) {
        ProjectLogger.log(
            "CacheManagementActor:clearCache : failed for map : " + mapName + " with error " + e,
            LoggerEnum.INFO);
        sender().tell(e, self());
      }

    } else {

      try {
        cache.clearAll();
        ProjectLogger.log("CacheManagementActor:clearALLCache ", LoggerEnum.INFO);
        response.setResponseCode(ResponseCode.success);
        sender().tell(response, self());
      } catch (Exception e) {
        ProjectLogger.log(
            "CacheManagementActor:clearALLCache " + " with error " + e, LoggerEnum.INFO);
        sender().tell(e, self());
      }
    }
  }
}
