package org.sunbird.user.actors;

import java.util.HashMap;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.service.IFeedService;
import org.sunbird.user.service.impl.FeedServiceImpl;

/** This class contains API related to user feed. */
@ActorConfig(
  tasks = {"getUserFeedById"},
  asyncTasks = {}
)
public class UserFeedActor extends BaseActor {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();
    if (ActorOperations.GET_USER_FEED_BY_ID.getValue().equalsIgnoreCase(operation)) {
      ProjectLogger.log(
          "UserFeedActor:onReceive getUserFeed method called", LoggerEnum.INFO.name());
      String userId = (String) request.getRequest().get(JsonKey.USER_ID);
      getUserFeed(userId);
    } else {
      onReceiveUnsupportedOperation("UserFeedActor");
    }
  }

  private void getUserFeed(String userId) {
    IFeedService feedService = FeedServiceImpl.getInstance();
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.USER_ID, userId);
    SearchDTO search = new SearchDTO();
    search.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Response userFeedResponse = feedService.search(search);
    sender().tell(userFeedResponse, self());
  }
}
