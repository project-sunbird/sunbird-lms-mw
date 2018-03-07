package org.sunbird.badge.actors;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.service.provider.ContentService;

import akka.actor.UntypedAbstractActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeNotifier extends UntypedAbstractActor {
	
	@Override
	public void onReceive(Object message) throws Throwable {
		if (message instanceof Request) {
			Request request = (Request) message;
			try {
				String operation = request.getOperation();
				switch (operation) {
				case "assignBadgeMessage":
					Response response = ContentService.assignBadge(request);
					sender().tell(response, getSelf());
					break;
				case "revokeBadgeMessage":
					System.out.println("This operation is not supported.");
					break;
				}

			} catch (Exception e) {
				sender().tell(e, self());
			}

		} else {
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
		}
	}

}
