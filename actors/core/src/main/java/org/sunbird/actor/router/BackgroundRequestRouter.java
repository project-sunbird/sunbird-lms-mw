package org.sunbird.actor.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseRouter;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.FromConfig;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BackgroundRequestRouter extends BaseRouter {

	private static String mode;
	private static ActorContext context = null;
	public static Map<String, ActorRef> routingMap = new HashMap<>();

	public BackgroundRequestRouter() {
		context = getContext();
		getMode();
	}

	public String getRouterMode() {
		return getMode();
	}

	public static String getMode() {
		if (StringUtils.isBlank(mode)) {
			String key = "background_actor_provider";
			mode = getPropertyValue(key);
		}
		return mode;
	}

	@Override
	public void route(Request request) throws Throwable {
		org.sunbird.common.request.ExecutionContext.setRequestId(request.getRequestId());
		ActorRef ref = routingMap.get(request.getOperation());
		if (null != ref) {
			ref.tell(request, ActorRef.noSender());
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}
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
			System.out.println(BackgroundRequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]");
			ProjectLogger.log(BackgroundRequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]", LoggerEnum.WARN.name());
			return false;
		}
		return true;
	}

}
