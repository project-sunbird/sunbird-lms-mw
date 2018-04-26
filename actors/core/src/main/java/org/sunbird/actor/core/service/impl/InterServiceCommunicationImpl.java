package org.sunbird.actor.core.service.impl;

import static akka.pattern.PatternsCS.ask;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.util.Timeout;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.sunbird.actor.core.service.InterServiceCommunication;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
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
      ActorSelection select = getRemoteRouter(BackgroundRequestRouter.class.getSimpleName());
      CompletionStage<ActorRef> futureActor =
          select.resolveOneCS(Duration.create(WAIT_TIME, "seconds"));
      try {
        actor = futureActor.toCompletableFuture().get();
      } catch (Exception e) {
        ProjectLogger.log(
            "InterServiceCommunicationImpl : getResponse - unable to get actorref from actorselection "
                + e.getMessage(),
            e);
      }
    }
    if (null == actor) {
      ProjectLogger.log(
          "InterServiceCommunicationImpl : getResponse - actorRef is null ", LoggerEnum.INFO);
      return obj;
    }
    CompletableFuture<Object> future = ask(actor, request, t).toCompletableFuture();
    try {
      obj = future.get(WAIT_TIME + 2, TimeUnit.SECONDS);
    } catch (Exception e) {
      ProjectLogger.log("Interservice communication error " + e.getMessage(), e);
    }
    return obj;
  }
}
