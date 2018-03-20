/**
 * 
 */
package org.sunbird.badge.actors;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

/**
 * @author Manzarul
 *
 */

@ActorConfig(tasks = { "createBadgeAssertion", "getBadgeAssertion", "getBadgeAssertionList",
		"revokeBadge" }, asyncTasks = {})
public class BadgeAssertionActor extends BaseActor {

	BadgingService service = BadgingFactory.getInstance();

	@Override
	public void onReceive(Request request) throws Throwable {
		ProjectLogger.log("BadgeAssertionActor onReceive called", LoggerEnum.INFO.name());
		String operation = request.getOperation();

		switch (operation) {
		case "createBadgeAssertion":
			createAssertion(request);
			break;
		case "getBadgeAssertion":
			getAssertionDetails(request);
			break;
		case "getBadgeAssertionList":
			getAssertionList(request);
			break;
		case "revokeBadge":
			revokeAssertion(request);
			break;
		default:
			onReceiveUnsupportedOperation("BadgeClassActor");
		}
	}

	/**
	 * This method will call the badger server to create badge assertion.
	 * 
	 * @param actorMessage
	 *            Request
	 */
	private void createAssertion(Request actorMessage) throws IOException {
		ProjectLogger.log("Got request to create badge assertion", actorMessage.getRequest(), LoggerEnum.INFO.name());
		Response result = service.badgeAssertion(actorMessage);
		ProjectLogger.log("resultMap==" + result.getResult());
		sender().tell(result, self());
		ProjectLogger.log("resultMapSent==" + result.getResult());
		Map<String, Object> map = BadgingUtil.createBadgeNotifierMap(result.getResult());
		Request request = new Request();
		String id = (String) actorMessage.getRequest().get(BadgingJsonKey.RECIPIENT_ID);
		String objectType = (String) actorMessage.getRequest().get(BadgingJsonKey.RECIPIENT_TYPE);
		ProjectLogger.log("Notifying badge assertion for " + objectType + " with id: " + id, actorMessage.getRequest(), LoggerEnum.INFO.name());
		// TODO: remove this after testing.
		if (StringUtils.isBlank(objectType)) {
			objectType = "content";
		}
		map.put(JsonKey.OBJECT_TYPE, objectType);
		map.put(JsonKey.ID, id);
		request.getRequest().putAll(map);
		request.setOperation(BadgeOperations.assignBadgeMessage.name());
		tellToAnother(request);
	}

	/**
	 * This method will get single assertion details based on issuerSlug,
	 * badgeClassSlug and assertionSlug
	 * 
	 * @param request
	 *            Request
	 */
	private void getAssertionDetails(Request request) throws IOException {
		Response result = service.getAssertionDetails(request);
		sender().tell(result, self());
	}

	/**
	 * This method will get single assertion details based on issuerSlug,
	 * badgeClassSlug and assertionSlug
	 * 
	 * @param request
	 *            Request
	 */
	private void getAssertionList(Request request) throws IOException {
		Response result = service.getAssertionList(request);
		sender().tell(result, self());
	}

	/**
	 * This method will make a call for revoking the badges.
	 * 
	 * @param request
	 *            Request
	 */
	private void revokeAssertion(Request request) throws IOException {
		Response result = service.revokeAssertion(request);
		sender().tell(result, self());
		Map<String, Object> map = BadgingUtil.createRevokeBadgeNotifierMap(request.getRequest());
		Request notificationReq = new Request();
		map.put(JsonKey.OBJECT_TYPE, request.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
		map.put(JsonKey.ID, request.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
		notificationReq.getRequest().putAll(map);
		notificationReq.setOperation(BadgeOperations.revokeBadgeMessage.name());
		tellToAnother(notificationReq);
	}
}
