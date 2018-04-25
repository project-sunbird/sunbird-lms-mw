package org.sunbird.actor.core.service.impl;

import static akka.pattern.PatternsCS.ask;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.util.Timeout;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.sunbird.actor.core.service.InterServiceCommunication;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.common.request.Request;
import scala.concurrent.duration.Duration;

/** Created by arvind on 24/4/18. */
public class InterServiceCommunicationImpl extends BaseMWService
    implements InterServiceCommunication {

  private Integer WAIT_TIME = 10;

  @Override
  public Object getResponse(Request request, String operation) {
    ActorRef actor = RequestRouter.getActor(operation);
    Timeout t = new Timeout(Duration.create(WAIT_TIME, TimeUnit.SECONDS));
    request.setOperation(operation);
    Object obj = null;
    if (null == actor) {
      ActorSelection select = getRemoteRouter(RequestRouter.class.getSimpleName());
      actor = (ActorRef) select.resolveOne(new Timeout(WAIT_TIME, TimeUnit.SECONDS));
    }
    CompletableFuture<Object> future = ask(actor, request, t).toCompletableFuture();

    try {
      obj = future.get(WAIT_TIME + 2, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return obj;
  }
}
