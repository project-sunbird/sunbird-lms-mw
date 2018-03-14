package org.sunbird.badge.actors;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.service.provider.ContentService;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeNotifier extends BaseActor {

	public static void init() {
		BackgroundRequestRouter.registerActor(BadgeNotifier.class,
				Arrays.asList(BadgeOperations.assignBadgeMessage.name(), BadgeOperations.revokeBadgeMessage.name()));
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		String operation = request.getOperation();
		String type = (String) request.getRequest().get("objectType");
		Response response;
		if (StringUtils.isNotBlank(operation) && StringUtils.isNotBlank(type)) {
			switch (operation) {
			case "assignBadgeMessage":
				response = assignBadge(type, request);
				break;
			case "revokeBadgeMessage":
				response = revokeBadge(type, request);
				break;
			default:
				response = new Response();
				break;
			}
		} else {
			response = new Response();
			response.setResponseCode(ResponseCode.CLIENT_ERROR);
		}
		sender().tell(response, getSelf());

	}

	private Response assignBadge(String type, Request request) throws Exception {
		Response response;
		switch (type.toUpperCase()) {
		case "USER":
			// TODO: user badge.
			response = new Response();
			break;
		case "CONTENT":
			response = ContentService.assignBadge(request);
			break;
		default:
			response = new Response();
			response.setResponseCode(ResponseCode.CLIENT_ERROR);
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
			response = new Response();
			response.setResponseCode(ResponseCode.CLIENT_ERROR);
			break;
		}
		return response;
	}

}
