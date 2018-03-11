package org.sunbird.learner.actors.badging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.AbstractBaseActor;
import org.sunbird.learner.util.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BadgeClassActor extends AbstractBaseActor {
    @Override
    public void onReceive(Object message) throws Throwable {
        ProjectLogger.log("BadgeClassActor onReceive called");

        if (message instanceof Request) {
            try {
                Request actorMessage = (Request) message;
                Util.initializeContext(actorMessage, JsonKey.USER);
                ExecutionContext.setRequestId(actorMessage.getRequestId());

                if (actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.CREATE_BADGE_CLASS.getValue())) {
                    createBadgeClass(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.GET_BADGE_CLASS.getValue())) {
                    getBadgeClass(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.LIST_BADGE_CLASS.getValue())) {
                    listBadgeClass(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.DELETE_BADGE_CLASS.getValue())) {
                    deleteBadgeClass(actorMessage);
                } else {
                    onReceiveUnsupportedOperation("BadgeClassActor");
                }
            } catch (Exception exception) {
                onReceiveException("BadgeClassActor", exception);
            }
        } else {
            onReceiveUnsupportedMessage("BadgeClassActor");
        }
    }

    private void createBadgeClass(Request actorMessage) {
        ProjectLogger.log("createBadgeClass called");

        try {
              Map<String, Object> requestData = actorMessage.getRequest();


            Map<String, String> formParams = (Map<String, String>) requestData.get(JsonKey.FORM_PARAMS);
            Map<String, byte[]> fileParams = (Map<String, byte[]>) requestData.get(JsonKey.FILE_PARAMS);

            String issuerSlug = formParams.remove(BadgingJsonKey.ISSUER_ID);
            String rootOrgId = formParams.remove(JsonKey.ROOT_ORG_ID);
            String type = formParams.remove(JsonKey.TYPE);
            String roles = formParams.remove(JsonKey.ROLES);

            String subtype = "";
            if (formParams.containsKey(JsonKey.SUBTYPE)) {
                subtype = formParams.remove(JsonKey.SUBTYPE);
            }

            Map<String, String> headers = BadgingUtil.getBadgrHeaders();

            String badgrResponseStr = HttpUtil.postFormData(formParams, fileParams, headers, BadgingUtil.getBadgeClassUrl(issuerSlug));

            Response response = new Response();
            BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, response.getResult());

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("createBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
        Response response = new Response();
        sender().tell(response, self());
    }

    private void getBadgeClass(Request actorMessage) {
        ProjectLogger.log("getBadgeClass called");

        try {
            Map<String, Object> requestData = actorMessage.getRequest();

            String issuerId = (String) requestData.get(BadgingJsonKey.ISSUER_ID);
            String badgeId = (String) requestData.get(BadgingJsonKey.BADGE_ID);

            Map<String, String> headers = BadgingUtil.getBadgrHeaders();
            String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerId, badgeId);

            String badgrResponseStr = HttpUtil.sendGetRequest(badgrUrl, headers);

            Response response = new Response();
            BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, response.getResult());

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("getBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
    }

    private void listBadgeClass(Request actorMessage) {
        ProjectLogger.log("listBadgeClass called");

        try {
            Map<String, Object> requestData = actorMessage.getRequest();
            List<String> issuerList = (List<String>) requestData.get(BadgingJsonKey.ISSUER_LIST);
            Map<String, Object> context = (Map<String, Object>) requestData.get(BadgingJsonKey.CONTEXT);

            List<Object> badges = new ArrayList<>();

            for (String issuerSlug : issuerList) {
                badges.addAll(listBadgeClassForIssuer(issuerSlug));
            }

            Response response = new Response();
            response.put(BadgingJsonKey.BADGES, badges);

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("listBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
    }


    private List<Object> listBadgeClassForIssuer(String issuerSlug) throws IOException {
        Map<String, String> headers = BadgingUtil.getBadgrHeaders();
        String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerSlug);

        String badgrResponseStr = HttpUtil.sendGetRequest(badgrUrl, headers);

        ObjectMapper mapper = new ObjectMapper();
        List<Object> filteredBadges = new ArrayList<>();
        List<Object> badges  = mapper.readValue(badgrResponseStr, ArrayList.class);

        for (Object badge : badges) {
            Map<String, Object> mappedBadge = new HashMap<>();
            BadgingUtil.prepareBadgeClassResponse((Map<String, Object>) badge, mappedBadge);
            filteredBadges.add(mappedBadge);
        }

        return filteredBadges;
    }

    private void deleteBadgeClass(Request actorMessage) {
        ProjectLogger.log("deleteBadgeClass called");

        try {
            Map<String, Object> requestData = actorMessage.getRequest();

            String issuerId = (String) requestData.get(BadgingJsonKey.ISSUER_ID);
            String badgeId = (String) requestData.get(BadgingJsonKey.BADGE_ID);

            Map<String, String> headers = BadgingUtil.getBadgrHeaders();
            String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerId, badgeId);

            String badgrResponseStr = HttpUtil.sendDeleteRequest(headers, badgrUrl);

            Response response = new Response();
            response.put(JsonKey.MESSAGE, badgrResponseStr.replaceAll("^\"|\"$", ""));

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("deleteBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
    }
}
