package org.sunbird.learner.actors.tac;

import java.sql.Timestamp;
import java.util.*;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.user.UserClient;
import org.sunbird.actorutil.user.impl.UserClientImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;

@ActorConfig(
  tasks = {"userTnCAccept"},
  asyncTasks = {}
)
public class UserTnCActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    ExecutionContext.setRequestId(request.getRequestId());

    if (operation.equalsIgnoreCase(ActorOperations.USER_TNC_ACCEPT.getValue())) {
      acceptTNC(request);
    } else {
      onReceiveUnsupportedOperation("UserTnCActor");
    }
  }

  private void acceptTNC(Request request) {
    String acceptedTnC = (String) request.getRequest().get(JsonKey.TNC_ACCEPTED_VERSION);
    Map<String, Object> userMap = new HashMap();
    String userId = (String) request.getContext().get(JsonKey.REQUESTED_BY);
    userMap.put(JsonKey.USER_ID, userId);
    userMap.put(JsonKey.TNC_ACCEPTED_VERSION, acceptedTnC);
    userMap.put(JsonKey.TNC_ACCEPTED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
    UserClient userClient = new UserClientImpl();

    userClient.updateUser(getActorRef(ActorOperations.UPDATE_USER.getValue()), userMap);

    Response response = new Response();
    response.getResult().put(JsonKey.RESULT, "SUCCESS");
    sender().tell(response, self());
  }
}
