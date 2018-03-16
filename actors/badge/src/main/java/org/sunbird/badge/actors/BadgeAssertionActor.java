/**
 * 
 */
package org.sunbird.badge.actors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;


/**
 * @author Manzarul
 *
 */
public class BadgeAssertionActor extends BaseActor {

	BadgingService service = BadgingFactory.getInstance();

	public static void init() {
		RequestRouter.registerActor(BadgeAssertionActor.class,
				Arrays.asList(BadgeOperations.createBadgeAssertion.name(), BadgeOperations.getBadgeAssertion.name(),
						BadgeOperations.getBadgeAssertionList.name(), BadgeOperations.revokeBadge.name()));
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		ProjectLogger.log("BadgeAssertionActor  onReceive called", LoggerEnum.INFO.name());
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
		Response result = service.badgeAssertion(actorMessage);
		sender().tell(result, self());
		Map<String, Object> map = BadgingUtil.createBadgeNotifierMap(result.getResult());
		Request request = new Request();
		map.put(JsonKey.OBJECT_TYPE, actorMessage.getRequest().get(JsonKey.OBJECT_TYPE));
		map.put(JsonKey.ID, actorMessage.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
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
		boolean response = BadgingUtil.matchAssertionData((String) request.getRequest().get(BadgingJsonKey.ISSUER_ID),
				(String) request.getRequest().get(BadgingJsonKey.BADGE_CLASS_ID), result.getResult());
		if (response) {
			sender().tell(result, self());
		} else {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.resourceNotFound.getErrorCode(),
					ResponseCode.resourceNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
			sender().tell(ex, self());
		}
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
		map.put(JsonKey.OBJECT_TYPE, request.getRequest().get(JsonKey.OBJECT_TYPE));
		map.put(JsonKey.ID, request.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
		notificationReq.getRequest().putAll(map);
		notificationReq.setOperation(BadgeOperations.revokeBadgeMessage.name());
		tellToAnother(notificationReq);
	}
}
