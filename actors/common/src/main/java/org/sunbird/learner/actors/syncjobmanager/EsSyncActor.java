package org.sunbird.learner.actors.syncjobmanager;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

/**
 * This class is used to sync the ElasticSearch and DB.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {"sync"},
  asyncTasks = {}
)
public class EsSyncActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String requestedOperation = request.getOperation();
    if (requestedOperation.equalsIgnoreCase(ActorOperations.SYNC.getValue())) {
      // return SUCCESS to controller and run the sync process in background
      Response response = new Response();
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      sender().tell(response, self());

      Request syncESRequest = new Request();
      syncESRequest.setOperation(ActorOperations.SYNC_ELASTIC_SEARCH.getValue());
      syncESRequest.getRequest().put(JsonKey.DATA, request.getRequest().get(JsonKey.DATA));
      try {
        tellToAnother(syncESRequest);
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occurred while syncing data to elastic search : ", ex);
      }
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }
}
