package org.sunbird.learner.util;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.BackgroundRequestRouterActor;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.actorutility.ActorSystemFactory;

/**
 * 
 * @author Amit Kumar
 *
 */
public final class ActorUtil {

  private ActorUtil() {}

  public static void tell(Request request) {

    // set telemetry context so that it could bbe accessible to the ackground actor as well ...
    request.getContext().put(JsonKey.TELEMETRY_CONTEXT, ExecutionContext.getCurrent().getRequestContext());

    if (null != BackgroundRequestRouterActor.routerMap
        && null != BackgroundRequestRouterActor.routerMap.get(request.getOperation())) {
      BackgroundRequestRouterActor.routerMap.get(request.getOperation()).tell(request,
          ActorRef.noSender());
    } else if (null != RequestRouterActor.routerMap
        && null != RequestRouterActor.routerMap.get(request.getOperation())) {
      RequestRouterActor.routerMap.get(request.getOperation()).tell(request, ActorRef.noSender());
    } else {
      Object obj =
          ActorSystemFactory.getActorSystem().initializeActorSystem(request.getOperation());
      if (obj instanceof ActorRef) {
        ProjectLogger
            .log("In ActorUtil(org.sunbird.learner.util) Actor ref is running " + ((ActorRef) obj));
        ((ActorRef) obj).tell(request, ActorRef.noSender());
      } else {
        ProjectLogger.log("In ActorUtil(org.sunbird.learner.util) Actor selection is running "
            + ((ActorSelection) obj));
        ((ActorSelection) obj).tell(request, ActorRef.noSender());
      }
    }
  }
}
