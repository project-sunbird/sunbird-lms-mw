package org.sunbird.badge.actors;

import java.util.Arrays;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.Request;
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
		switch (operation) {
		case "assignBadgeMessage":
			Response response = ContentService.assignBadge(request);
			sender().tell(response, getSelf());
			break;
		case "revokeBadgeMessage":
			System.out.println("This operation is not supported.");
			break;
		}
	}

}
