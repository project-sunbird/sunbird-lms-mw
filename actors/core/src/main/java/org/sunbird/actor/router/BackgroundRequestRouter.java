package org.sunbird.actor.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.routing.FromConfig;

public class BackgroundRequestRouter extends UntypedAbstractActor {

	private static akka.actor.ActorContext context = null;
	public static Map<String, ActorRef> routingMap = new HashMap<>();

	public BackgroundRequestRouter() {
		context = getContext();
	}
	
	public static void registerActor(Class<?> clazz, List<String> operations) {
		try {
			ActorRef actor = context.actorOf(FromConfig.getInstance().props(Props.create(clazz)), clazz.getSimpleName());
			for (String operation : operations) {
				routingMap.put(operation, actor);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onReceive(Object message) throws Throwable {
		if (message instanceof Request) {
			ProjectLogger.log("BackgroundRequestRouterActor onReceive called");
			Request actorMessage = (Request) message;
			org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
			ActorRef ref = routingMap.get(actorMessage.getOperation());
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
