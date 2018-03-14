package org.sunbird.badge.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.model.BadgeClassExtension;
import org.sunbird.badge.service.BadgeClassExtensionService;
import org.sunbird.badge.service.impl.BadgeClassExtensionServiceImpl;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

public class BadgeClassActor extends BaseActor {
    BadgeClassExtensionService badgeClassExtensionService = new BadgeClassExtensionServiceImpl();

    public static void init() {
        RequestRouter.registerActor(BadgeClassActor.class, Arrays.asList(
                BadgeOperations.createBadgeClass.name(),
                BadgeOperations.getBadgeClass.name(),
                BadgeOperations.listBadgeClass.name(),
                BadgeOperations.deleteBadgeClass.name()));
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
            Map<String, Object> requestData = actorMessage.getRequest();

            Map<String, String> formParams = (Map<String, String>) requestData.get(JsonKey.FORM_PARAMS);
            Map<String, byte[]> fileParams = (Map<String, byte[]>) requestData.get(JsonKey.FILE_PARAMS);

            String issuerId = formParams.remove(BadgingJsonKey.ISSUER_ID);
            String rootOrgId = formParams.remove(JsonKey.ROOT_ORG_ID);
            String type = formParams.remove(JsonKey.TYPE);
            String roles = formParams.remove(JsonKey.ROLES);
            List<String> rolesList = new ArrayList<>();
            if (roles != null) {
                ObjectMapper mapper = new ObjectMapper();
                rolesList = mapper.readValue(roles, ArrayList.class);
            }

            String subtype = "";
            if (formParams.containsKey(JsonKey.SUBTYPE)) {
                subtype = formParams.remove(JsonKey.SUBTYPE);
            }

            Map<String, String> headers = BadgingUtil.getBadgrHeaders();

            HttpUtilResponse httpUtilResponse = HttpUtil.postFormData(formParams, fileParams, headers, BadgingUtil.getBadgeClassUrl(issuerId));
            String badgrResponseStr = httpUtilResponse.getBody();
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> badgrResponseMap = (Map<String, Object>) mapper.readValue(badgrResponseStr, HashMap.class);
            String badgeId = (String) badgrResponseMap.get(BadgingJsonKey.SLUG);

            BadgeClassExtension badgeClassExt = new BadgeClassExtension(badgeId, issuerId, rootOrgId, type, subtype, rolesList);
            badgeClassExtensionService.save(badgeClassExt);

            Response response = new Response();
            BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, badgeClassExt, response.getResult());

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

            BadgeClassExtension badgeClassExtension = badgeClassExtensionService.get(badgeId);

            Response response = new Response();
            BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, badgeClassExtension, response.getResult());

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
            String rootOrgId = (String) context.get(JsonKey.ROOT_ORG_ID);
            String type = (String) context.get(JsonKey.TYPE);
            String subtype = (String) context.get(JsonKey.SUBTYPE);
            List<String> allowedRoles = (List<String>) context.get(JsonKey.ROLES);

            List<BadgeClassExtension> badgeClassExtList = badgeClassExtensionService.get(issuerList,
                    rootOrgId, type, subtype, allowedRoles);
            List<String> filteredIssuerList = badgeClassExtList.stream()
                    .map(badge -> badge.getIssuerId()).distinct().collect(Collectors.toList());

            List<Object> badges = new ArrayList<>();

            for (String issuerSlug : filteredIssuerList) {
                badges.addAll(listBadgeClassForIssuer(issuerSlug, badgeClassExtList));
            }

            Response response = new Response();
            response.put(BadgingJsonKey.BADGES, badges);

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("listBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
    }


    private List<Object> listBadgeClassForIssuer(String issuerSlug, List<BadgeClassExtension> badgeClassExtensionList) throws IOException {
        Map<String, String> headers = BadgingUtil.getBadgrHeaders();
        String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerSlug);

        String badgrResponseStr = HttpUtil.sendGetRequest(badgrUrl, headers);

        ObjectMapper mapper = new ObjectMapper();
        List<Object> filteredBadges = new ArrayList<>();
        List<Map<String, Object>> badges = mapper.readValue(badgrResponseStr, ArrayList.class);

        for (Map<String, Object> badge : badges) {
            BadgeClassExtension matchedBadgeClassExt = badgeClassExtensionList.stream()
                    .filter(x -> x.getBadgeId().equals(badge.get(BadgingJsonKey.SLUG))).findFirst()
                    .orElse(null);

            if (matchedBadgeClassExt != null) {
                Map<String, Object> mappedBadge = new HashMap<>();
                BadgingUtil.prepareBadgeClassResponse(badge, matchedBadgeClassExt, mappedBadge);
                filteredBadges.add(mappedBadge);
            }
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

            HttpUtilResponse httpUtilResponse = HttpUtil.sendDeleteRequest(headers, badgrUrl);
            String badgrResponseStr = httpUtilResponse.getBody();
            badgeClassExtensionService.delete(badgeId);
            Response response = new Response();
            response.put(JsonKey.MESSAGE, badgrResponseStr.replaceAll("^\"|\"$", ""));

            sender().tell(response, self());
        } catch (IOException e) {
            ProjectLogger.log("deleteBadgeClass: exception = ", e);

            sender().tell(e, self());
        }
    }
}
