package org.sunbird.badge.actors;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.service.ContentService;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeNotifier extends BaseActor {

	private static final String INVALID_BADGE_ASSIGN_REQUEST = "INVALID_BADGE_ASSIGN_REQUEST";
	private static final String INVALID_BADGE_REVOKE_REQUEST = "INVALID_BADGE_REVOKE_REQUEST";

	public static void init() {
		BackgroundRequestRouter.registerActor(BadgeNotifier.class,
				Arrays.asList(BadgeOperations.assignBadgeMessage.name(), BadgeOperations.revokeBadgeMessage.name()));
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		String operation = request.getOperation();
		String type = (String) request.getRequest().get(JsonKey.OBJECT_TYPE);
		Response response;
		if (StringUtils.isNotBlank(operation) && StringUtils.isNotBlank(type)) {
			switch (operation) {
			case "assignBadgeMessage":
				response = assignBadge(type, request);
				sender().tell(response, getSelf());
				break;
			case "revokeBadgeMessage":
				response = revokeBadge(type, request);
				sender().tell(response, getSelf());
				break;
			default:
				onReceiveUnsupportedOperation(request.getOperation());
				break;
			}
		} else {
			onReceiveUnsupportedMessage(request.getOperation());
		}

	}

	private Response assignBadge(String type, Request request) throws Exception {
		Response response;
		switch (type.toUpperCase()) {
		case "USER":
			response = new Response();

			break;
		case "CONTENT":
			response = ContentService.assignBadge(request);
			break;
		default:
			response = invalidObjectType(INVALID_BADGE_ASSIGN_REQUEST);
			break;
		}
		return response;
	}

	private Response revokeBadge(String type, Request request) throws Exception {
		Response response;
		switch (type.toUpperCase()) {
		case "USER":
			// TODO: user badge.
			response = new Response();
			break;
		case "CONTENT":
			response = ContentService.revokeBadge(request);
			break;
		default:
			response = invalidObjectType(INVALID_BADGE_REVOKE_REQUEST);
			break;
		}
		return response;
	}

	private Response invalidObjectType(String error) {
		Response response = new Response();
		response.setResponseCode(ResponseCode.CLIENT_ERROR);
		ResponseParams params = new ResponseParams();
		params.setErrmsg("ObjectType is invalid.");
		params.setErr(error);
		response.setParams(params);
		return response;
	}

}
