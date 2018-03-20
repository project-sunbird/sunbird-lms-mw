package org.sunbird.badge.actors;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.response.ResponseParams;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.service.ContentService;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

@ActorConfig(tasks = {}, asyncTasks = { "assignBadgeMessage", "revokeBadgeMessage" })
public class BadgeNotifier extends BaseActor {

	private static final String INVALID_BADGE_ASSIGN_REQUEST = "INVALID_BADGE_ASSIGN_REQUEST";
	private static final String INVALID_BADGE_REVOKE_REQUEST = "INVALID_BADGE_REVOKE_REQUEST";

	@Override
	public void onReceive(Request request) throws Throwable {
		String operation = request.getOperation();
		String type = (String) request.getRequest().get(JsonKey.OBJECT_TYPE);
		ProjectLogger.log("Processing badge notification.", request.getRequest().put("operation", operation), LoggerEnum.INFO.name());
		Response response;
		if (StringUtils.isNotBlank(operation) && StringUtils.isNotBlank(type)) {
			switch (operation) {
			case "assignBadgeMessage":
				response = assignBadge(type, request);
				sender().tell(response, self());
				break;
			case "revokeBadgeMessage":
				response = revokeBadge(type, request);
				sender().tell(response, self());
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
			request.setOperation(BadgeOperations.assignBadgeToUser.name());
			tellToAnother(request);
			response = new Response();
			response.setResponseCode(ResponseCode.success);
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
			request.setOperation(BadgeOperations.revokeBadgeFromUser.name());
			tellToAnother(request);
			response = new Response();
			response.setResponseCode(ResponseCode.success);
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
		params.setErrmsg("ObjectType is invalid to assign/revoke badge.");
		params.setErr(error);
		response.setParams(params);
		return response;
	}

}
