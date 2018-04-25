package org.sunbird.learner.actors.bulkupload.service.impl;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.pattern.Patterns;
import akka.util.Timeout;
import java.util.concurrent.TimeUnit;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.service.InterServiceCommunication;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/** Created by arvind on 24/4/18. */
public class InterServiceCommunicationImpl extends BaseMWService
    implements InterServiceCommunication {

  private Integer WAIT_TIME = 50;

  @Override
  public Object getResponse(Request request, String operation) {
    ActorRef actor = RequestRouter.getActor(operation);
    Object obj = null;
    if (null == actor) {
      ActorSelection select = getRemoteRouter(RequestRouter.class.getSimpleName());
      actor = (ActorRef) select.resolveOne(new Timeout(10, TimeUnit.SECONDS));
    }
    Timeout timeout = new Timeout(Duration.create(WAIT_TIME, "seconds"));
    Future<Object> future = Patterns.ask(actor, request, timeout);
    try {
      obj = Await.result(future, timeout.duration());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return obj;
  }
}
