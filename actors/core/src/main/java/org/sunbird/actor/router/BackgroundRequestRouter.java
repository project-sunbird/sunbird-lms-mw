package org.sunbird.actor.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.request.Request;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.FromConfig;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BackgroundRequestRouter extends BaseActor {

	private static akka.actor.ActorContext context = null;
	private static Map<String, ActorRef> routingMap = new HashMap<>();

	public BackgroundRequestRouter() {
		context = getContext();
	}

	public static void registerActor(Class<?> clazz, List<String> operations) {
		try {
			ActorRef actor = context.actorOf(FromConfig.getInstance().props(Props.create(clazz)),
					clazz.getSimpleName());
			System.out.println("Actor path: " + actor.path());
			for (String operation : operations) {
				routingMap.put(operation, actor);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		org.sunbird.common.request.ExecutionContext.setRequestId(request.getRequestId());
		ActorRef ref = routingMap.get(request.getOperation());
		if (null != ref) {
			ref.tell(request, ActorRef.noSender());
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}

	}
}
