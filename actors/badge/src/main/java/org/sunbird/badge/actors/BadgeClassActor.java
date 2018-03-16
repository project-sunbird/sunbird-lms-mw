package org.sunbird.badge.actors;

import java.util.Arrays;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

public class BadgeClassActor extends BaseActor {
    private BadgingService badgingService;

    public static void init() {
        RequestRouter.registerActor(BadgeClassActor.class, Arrays.asList(
                BadgeOperations.createBadgeClass.name(),
                BadgeOperations.getBadgeClass.name(),
                BadgeOperations.listBadgeClass.name(),
                BadgeOperations.deleteBadgeClass.name()));
    }

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
            case "listBadgeClass":
                listBadgeClass(request);
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

    private void listBadgeClass(Request actorMessage) {
        ProjectLogger.log("listBadgeClass called");

        try {
            Response response = badgingService.getBadgeClassList(actorMessage);

            sender().tell(response, self());
        } catch (ProjectCommonException e) {
            ProjectLogger.log("listBadgeClass: exception = ", e);

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
