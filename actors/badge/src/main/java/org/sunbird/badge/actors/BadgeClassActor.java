package org.sunbird.badge.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

@ActorConfig(tasks = { "createBadgeClass", "getBadgeClass", "searchBadgeClass", "deleteBadgeClass" }, asyncTasks = {})
public class BadgeClassActor extends BaseActor {
	private BadgingService badgingService;

	public BadgeClassActor() {
		this.badgingService = BadgingFactory.getInstance();
	}

	public BadgeClassActor(BadgingService badgingService) {
		this.badgingService = badgingService;
	}

	@Override
	public void onReceive(Request request) {
		ProjectLogger.log("BadgeClassActor onReceive called");
		String operation = request.getOperation();

		switch (operation) {
		case "createBadgeClass":
			createBadgeClass(request);
			break;
		case "getBadgeClass":
			getBadgeClass(request);
			break;
		case "searchBadgeClass":
			searchBadgeClass(request);
			break;
		case "deleteBadgeClass":
			deleteBadgeClass(request);
			break;
		default:
			onReceiveUnsupportedOperation("BadgeClassActor");
		}
	}

	private void createBadgeClass(Request actorMessage) {
		ProjectLogger.log("createBadgeClass called");

		try {
			Response response = badgingService.createBadgeClass(actorMessage);

			sender().tell(response, self());
		} catch (ProjectCommonException e) {
			ProjectLogger.log("createBadgeClass: exception = ", e);

			sender().tell(e, self());
		}
	}

	private void getBadgeClass(Request actorMessage) {
		ProjectLogger.log("getBadgeClass called");

		try {
			Response response = badgingService.getBadgeClassDetails(actorMessage);

			sender().tell(response, self());
		} catch (ProjectCommonException e) {
			ProjectLogger.log("getBadgeClass: exception = ", e);

			sender().tell(e, self());
		}
	}

	private void searchBadgeClass(Request actorMessage) {
		ProjectLogger.log("searchBadgeClass called");

		try {
			Response response = badgingService.searchBadgeClass(actorMessage);

			sender().tell(response, self());
		} catch (ProjectCommonException e) {
			ProjectLogger.log("searchBadgeClass: exception = ", e);

			sender().tell(e, self());
		}
	}

	private void deleteBadgeClass(Request actorMessage) {
		ProjectLogger.log("deleteBadgeClass called");

		try {
			Response response = badgingService.removeBadgeClass(actorMessage);

			sender().tell(response, self());
		} catch (ProjectCommonException e) {
			ProjectLogger.log("deleteBadgeClass: exception = ", e);

			sender().tell(e, self());
		}
	}
}
