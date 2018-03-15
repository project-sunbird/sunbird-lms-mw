package org.sunbird.actor.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseRouter;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.routing.FromConfig;
import akka.util.Timeout;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class RequestRouter extends BaseRouter {

	private static String mode;
	private static final int WAIT_TIME_VALUE = 9;
	private static ActorContext context;
	public static Map<String, ActorRef> routingMap = new HashMap<>();

	public RequestRouter() {
		context = getContext();
		getMode();
	}

	public String getRouterMode() {
		return getMode();
	}

	public static String getMode() {
		if (StringUtils.isBlank(mode)) {
			mode = getPropertyValue(JsonKey.API_ACTOR_PROVIDER);
		}
		return mode;
	}

	@Override
	public void route(Request request) throws Throwable {
		org.sunbird.common.request.ExecutionContext.setRequestId(request.getRequestId());
		ActorRef ref = routingMap.get(request.getOperation());
		if (null != ref) {
			route(ref, request, getContext().dispatcher());
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
			System.out.println(RequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]");
			ProjectLogger.log(RequestRouter.class.getSimpleName()
					+ " context is not available to initialise actor for [" + name + "]", LoggerEnum.WARN.name());
			return false;
		}
		return true;
	}

	/**
	 * method will route the message to corresponding router pass into the argument
	 * .
	 *
	 * @param router
	 * @param message
	 * @return boolean
	 */
	private boolean route(ActorRef router, Request message, ExecutionContext ec) {
		long startTime = System.currentTimeMillis();
		ProjectLogger.log("Actor Service Call start  for  api ==" + message.getOperation() + " start time " + startTime,
				LoggerEnum.PERF_LOG);
		Timeout timeout = new Timeout(Duration.create(WAIT_TIME_VALUE, TimeUnit.SECONDS));
		Future<Object> future = Patterns.ask(router, message, timeout);
		ActorRef parent = sender();
		future.onComplete(new OnComplete<Object>() {
			@Override
			public void onComplete(Throwable failure, Object result) {
				if (failure != null) {
					ProjectLogger.log("Actor Service Call Ended on Failure for  api ==" + message.getOperation()
							+ " end time " + System.currentTimeMillis() + "  Time taken "
							+ (System.currentTimeMillis() - startTime), LoggerEnum.PERF_LOG);
					// We got a failure, handle it here
					ProjectLogger.log(failure.getMessage(), failure);
					if (failure instanceof ProjectCommonException) {
						parent.tell(failure, self());
					} else {
						ProjectCommonException exception = new ProjectCommonException(
								ResponseCode.internalError.getErrorCode(), ResponseCode.internalError.getErrorMessage(),
								ResponseCode.CLIENT_ERROR.getResponseCode());
						parent.tell(exception, self());
					}
				} else {
					// We got a result, handle it
					ProjectLogger.log("Actor Service Call Ended on Success for  api ==" + message.getOperation()
							+ " end time " + System.currentTimeMillis() + "  Time taken "
							+ (System.currentTimeMillis() - startTime), LoggerEnum.PERF_LOG);
					parent.tell(result, self());
					// Audit log method call
					if (result instanceof Response && Util.auditLogUrlMap.containsKey(message.getOperation())) {
						AuditOperation auditOperation = (AuditOperation) Util.auditLogUrlMap
								.get(message.getOperation());
						Map<String, Object> map = new HashMap<>();
						map.put(JsonKey.OPERATION, auditOperation);
						map.put(JsonKey.REQUEST, message);
						map.put(JsonKey.RESPONSE, result);
						Request request = new Request();
						request.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
						request.setRequest(map);
						SunbirdMWService.tell(request, self());
					}
				}
			}
		}, ec);
		return true;
	}

}
