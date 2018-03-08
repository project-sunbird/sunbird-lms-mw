package org.sunbird.actor.router;

import java.util.List;

import org.sunbird.actor.core.BaseRouter;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.FromConfig;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class RequestRouter extends BaseRouter {

	protected static ActorContext context = null;

	public RequestRouter() {
		context = getContext();
	}

	public static void registerActor(Class<?> clazz, List<String> operations) {
		if (!contextAvailable(clazz.getSimpleName()))
			return;
		try {
			ActorRef actor = context.actorOf(FromConfig.getInstance().props(Props.create(clazz)),
					clazz.getSimpleName());
			for (String operation : operations) {
				routingMap.put(operation, actor);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static boolean contextAvailable(String name) {
		if (null == context) {
			System.out.println(RequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]");
			ProjectLogger.log(RequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]", LoggerEnum.WARN.name());
			return false;
		}
		return true;
	}

}
