package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;
import akka.actor.UntypedAbstractActor;

public class BackgroundRequestRouterActor extends UntypedAbstractActor {

	public static Map<String, ActorRef> routerMap = new HashMap<>();

	/**
	 * constructor to initialize router actor with child actor pool
	 */
	public BackgroundRequestRouterActor() {

		initializeRouterMap();
	}

	/**
	 * Initialize the map with operation as key and corresponding router as value.
	 */
	private void initializeRouterMap() {
		
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Request) {
			ProjectLogger.log("BackgroundRequestRouterActor onReceive called");
			Request actorMessage = (Request) message;
			org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
			ActorRef ref = routerMap.get(actorMessage.getOperation());
			if (null != ref) {
				ref.tell(message, ActorRef.noSender());
			} else {
				ProjectLogger.log("UNSUPPORTED OPERATION TYPE");
				ProjectCommonException exception = new ProjectCommonException(
						ResponseCode.invalidOperationName.getErrorCode(),
						ResponseCode.invalidOperationName.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, ActorRef.noSender());
			}
		} else {
			ProjectLogger.log("UNSUPPORTED MESSAGE");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(exception, ActorRef.noSender());
		}

	}
}
