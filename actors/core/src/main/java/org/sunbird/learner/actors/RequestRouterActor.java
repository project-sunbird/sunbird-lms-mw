package org.sunbird.learner.actors;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BadgingActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.badges.BadgeAssertionActor;
import org.sunbird.learner.actors.badges.BadgesActor;
import org.sunbird.learner.actors.badging.BadgeClassActor;
import org.sunbird.learner.actors.badging.BadgeIssuerActor;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.Util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.routing.FromConfig;
import akka.util.Timeout;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * @author Amit Kumar
 * @author arvind .
 *
 *         Class to initialize and select the appropriate actor on the basis of
 *         message type .
 */
public class RequestRouterActor extends UntypedAbstractActor {

	private static ActorSystem system = null;
	private static ActorSelection selection = null;
	private static final String ACTOR_CONFIG_NAME = "RemoteMWConfig";
	private static final String REMOTE_ACTOR_SYSTEM_NAME = "RemoteMiddlewareActorSystem";

	

	// Badging Actors
	private ActorRef badgesActor;
	private ActorRef badgeClassActor;
	private ActorRef badgeAssertionActor;
	private ActorRef badgeIssuerActor;

	private ExecutionContext ec;

	public static final Map<String, ActorRef> routerMap = new HashMap<>();
	private static final int WAIT_TIME_VALUE = 9;
	

	// Badging Actor Constants
	private static final String BADGES_ACTOR = "badgesActor";
	private static final String BADGE_ISSUER_ACTOR = "badgeIssuerActor";
	private static final String BADGE_CLASS_ACTOR = "badgeClassActor";
	private static final String BADGE_ASSERTION_ACTOR = "badgeAssertionActor";

	/**
	 * @return the system
	 */
	public static ActorSystem getSystem() {
		return system;
	}

	/**
	 * @param system
	 *            the system to set
	 */
	public static void setSystem(ActorSystem system) {
		RequestRouterActor.system = system;
	}

	/**
	 * constructor to initialize router actor with child actor pool
	 */
	public RequestRouterActor() {

		badgesActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(BadgesActor.class)),
				BADGES_ACTOR);

		// Badging Actors
		badgeIssuerActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(BadgeIssuerActor.class)),
				BADGE_ISSUER_ACTOR);
		badgeClassActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(BadgeClassActor.class)),
				BADGE_CLASS_ACTOR);
		badgeAssertionActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(BadgeAssertionActor.class)), BADGE_ASSERTION_ACTOR);

		ec = getContext().dispatcher();
		initializeRouterMap();
	}

	/**
	 * Initialize the map with operation as key and corresponding router as value.
	 */
	private void initializeRouterMap() {
		
		
		
		
		routerMap.put(ActorOperations.GET_ALL_BADGE.getValue(), badgesActor);
		routerMap.put(ActorOperations.ADD_USER_BADGE.getValue(), badgesActor);
		routerMap.put(ActorOperations.HEALTH_CHECK.getValue(), badgesActor);
		routerMap.put(ActorOperations.ACTOR.getValue(), badgesActor);
		routerMap.put(ActorOperations.ES.getValue(), badgesActor);
		routerMap.put(ActorOperations.CASSANDRA.getValue(), badgesActor);
		
		

		

		routerMap.put(BadgingActorOperations.CREATE_BADGE_ASSERTION.getValue(), badgeAssertionActor);
		routerMap.put(BadgingActorOperations.GET_BADGE_ASSERTION.getValue(), badgeAssertionActor);
		routerMap.put(BadgingActorOperations.GET_BADGE_ASSERTION_LIST.getValue(), badgeAssertionActor);
		routerMap.put(BadgingActorOperations.REVOKE_BADGE.getValue(), badgeAssertionActor);
		routerMap.put(BadgingActorOperations.CREATE_BADGE_ISSUER.getValue(), badgeIssuerActor);

		routerMap.put(BadgingActorOperations.CREATE_BADGE_CLASS.getValue(), badgeClassActor);
		routerMap.put(BadgingActorOperations.GET_BADGE_CLASS.getValue(), badgeClassActor);
		routerMap.put(BadgingActorOperations.LIST_BADGE_CLASS.getValue(), badgeClassActor);
		routerMap.put(BadgingActorOperations.DELETE_BADGE_CLASS.getValue(), badgeClassActor);
		routerMap.put(BadgingActorOperations.GET_BADGE_ISSUER.getValue(), badgeIssuerActor);
		routerMap.put(BadgingActorOperations.GET_ALL_ISSUER.getValue(), badgeIssuerActor);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Request) {
			ProjectLogger.log("Actor selector onReceive called");
			Request actorMessage = (Request) message;
			org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
			ActorRef ref = routerMap.get(actorMessage.getOperation());
			if (null != ref) {
				route(ref, actorMessage);
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

	/**
	 * method will route the message to corresponding router pass into the argument
	 * .
	 *
	 * @param router
	 * @param message
	 * @return boolean
	 */
	private boolean route(ActorRef router, Request message) {
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
						parent.tell(failure, ActorRef.noSender());
					} else {
						ProjectCommonException exception = new ProjectCommonException(
								ResponseCode.internalError.getErrorCode(), ResponseCode.internalError.getErrorMessage(),
								ResponseCode.CLIENT_ERROR.getResponseCode());
						parent.tell(exception, ActorRef.noSender());
					}
				} else {
					ProjectLogger.log("PARENT RESULT IS " + result);
					// We got a result, handle it
					ProjectLogger.log("Actor Service Call Ended on Success for  api ==" + message.getOperation()
							+ " end time " + System.currentTimeMillis() + "  Time taken "
							+ (System.currentTimeMillis() - startTime), LoggerEnum.PERF_LOG);
					parent.tell(result, ActorRef.noSender());
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
//						auditLogManagementActor.tell(request, self());
					}
				}
			}
		}, ec);
		return true;
	}

	public static void createConnectionForBackgroundActors() {
		String path = PropertiesCache.getInstance().getProperty("background.remote.actor.path");
		ActorSystem system = null;
		String bkghost = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP);
		String bkgport = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT);
		Config con = null;
		System.out.println("api_actor_provider: " + PropertiesCache.getInstance().getProperty("api_actor_provider"));
		System.out.println("bkghost: " + bkghost);
		System.out.println("bkgport: " + bkgport);
		if ("local".equalsIgnoreCase(PropertiesCache.getInstance().getProperty("api_actor_provider"))) {
			con = ConfigFactory.load().getConfig(ACTOR_CONFIG_NAME);
			system = ActorSystem.create(REMOTE_ACTOR_SYSTEM_NAME, con);
		} else {
			system = RequestRouterActor.getSystem();
		}
		try {
			if (!ProjectUtil.isStringNullOREmpty(bkghost) && !ProjectUtil.isStringNullOREmpty(bkgport)) {
				ProjectLogger.log("value is taking from system env");
				path = MessageFormat.format(
						PropertiesCache.getInstance().getProperty("background.remote.actor.env.path"),
						System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP),
						System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT));
			}
			ProjectLogger.log("Actor path is ==" + path, LoggerEnum.INFO.name());
		} catch (Exception e) {
			ProjectLogger.log(e.getMessage(), e);
		}
		selection = system.actorSelection(path);
		ProjectLogger.log("ActorUtility selection reference    : " + selection);
	}

	/**
	 * @return the selection
	 */
	public static ActorSelection getSelection() {
		if (null == selection) {
			createConnectionForBackgroundActors();
		}
		return selection;
	}

}
