package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.BackgroundRequestRouterActor;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.actorutility.ActorSystemFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * 
 * @author Amit Kumar
 *
 */
public final class ActorUtil {

	private ActorUtil() {
	}

	public static void tell(Request request) {

		// set telemetry context so that it could bbe accessible to the ackground actor
		// as well ...
		request.getContext().put(JsonKey.TELEMETRY_CONTEXT, ExecutionContext.getCurrent().getRequestContext());
		String operation = request.getOperation();

		if (null != BackgroundRequestRouterActor.routerMap.get(operation)) {
			BackgroundRequestRouterActor.routerMap.get(operation).tell(request, ActorRef.noSender());
		} else if (null != RequestRouterActor.routerMap.get(operation)) {
			RequestRouterActor.routerMap.get(request.getOperation()).tell(request, ActorRef.noSender());
		} else {
			Object obj = ActorSystemFactory.getActorSystem().initializeActorSystem(operation);
			if (obj instanceof ActorRef) {
				((ActorRef) obj).tell(request, ActorRef.noSender());
			} else {
				((ActorSelection) obj).tell(request, ActorRef.noSender());
			}
		}
	}
}
