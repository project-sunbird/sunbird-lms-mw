package org.sunbird.badge.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.MapperUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import org.sunbird.badge.model.BadgeClassExtension;

public class BadgingUtil {

    public static final String SUNBIRD_BADGR_SERVER_URL_DEFAULT = "http://localhost:8000";
    public static final String BADGING_AUTHORIZATION_FORMAT = "Token %s";
    public static final String  SUNBIRD_BADGER_CREATE_ASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertions";
    public static final String  SUNBIRD_BADGER_GETASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertions/{2}";
    public static final String  SUNBIRD_BADGER_GETALLASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion";
    public static final String  SUNBIRD_BADGER_REVOKE_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion/{2}";

    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();


    public static String getBadgrBaseUrl() {
        String badgeraseUrl= SUNBIRD_BADGR_SERVER_URL_DEFAULT;
        if(!ProjectUtil.isStringNullOREmpty(System.getenv(BadgingJsonKey.BADGER_BASE_URL))){
            badgeraseUrl = System.getenv(BadgingJsonKey.BADGER_BASE_URL);
        }else if(!ProjectUtil.isStringNullOREmpty(propertiesCache.getProperty(BadgingJsonKey.BADGER_BASE_URL))){
            badgeraseUrl = propertiesCache.getProperty(BadgingJsonKey.BADGER_BASE_URL);
        }
        return badgeraseUrl;
    }

    public static String getBadgeClassUrl(String issuerSlug) {
        return String.format("%s/v1/issuer/issuers/%s/badges", getBadgrBaseUrl(), issuerSlug);
    }

    public static String getBadgeClassUrl(String issuerSlug, String badgeSlug) {
        return String.format("%s/v1/issuer/issuers/%s/badges/%s", getBadgrBaseUrl(), issuerSlug, badgeSlug);
    }

    public static Map<String, String> getBadgrHeaders() {
        HashMap<String, String> headers = new HashMap<>();

        String authToken = System.getenv(BadgingJsonKey.BADGING_AUTHORIZATION_KEY);
        if(ProjectUtil.isStringNullOREmpty(authToken)){
            authToken =  propertiesCache.getProperty(BadgingJsonKey.BADGING_AUTHORIZATION_KEY);
        }
        if (!StringUtils.isBlank(authToken)) {
            headers.put("Authorization", String.format(BADGING_AUTHORIZATION_FORMAT, authToken));
        }
        headers.put("Accept", "application/json");
        return headers;
    }

    public static String getLastSegment(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static void prepareBadgeClassResponse(Map<String, Object> inputMap, BadgeClassExtension badgeClassExtension, Map<String, Object> outputMap) throws IOException {
        MapperUtil.put(inputMap, BadgingJsonKey.CREATED_AT, outputMap, JsonKey.CREATED_DATE);
        MapperUtil.put(inputMap, BadgingJsonKey.SLUG, outputMap, BadgingJsonKey.BADGE_ID);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_ID, outputMap, BadgingJsonKey.BADGE_ID_URL);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_ISSUER, outputMap, BadgingJsonKey.ISSUER_ID_URL);

        if (outputMap.containsKey(BadgingJsonKey.ISSUER_ID_URL)) {
            outputMap.put(BadgingJsonKey.ISSUER_ID, getLastSegment((String) outputMap.get(BadgingJsonKey.ISSUER_ID_URL)));
        }

        MapperUtil.put(inputMap, JsonKey.NAME, outputMap, JsonKey.NAME);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_DESCRIPTION, outputMap, JsonKey.DESCRIPTION);
        MapperUtil.put(inputMap, JsonKey.IMAGE, outputMap, JsonKey.IMAGE);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_CRITERIA, outputMap, JsonKey.CRITERIA);

        MapperUtil.put(inputMap, BadgingJsonKey.RECIPIENT_COUNT, outputMap, JsonKey.RECIPIENT_COUNT);

        if (badgeClassExtension != null) {
            outputMap.put(JsonKey.ROOT_ORG_ID, badgeClassExtension.getRootOrgId());
            outputMap.put(JsonKey.TYPE, badgeClassExtension.getType());
            outputMap.put(JsonKey.SUBTYPE, badgeClassExtension.getSubtype());
            outputMap.put(JsonKey.ROLES, badgeClassExtension.getRoles());
        }
    }

    public static void prepareBadgeClassResponse(String inputJson, BadgeClassExtension badgeClassExtension, Map<String, Object> outputMap) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String , Object> inputMap  = mapper.readValue(inputJson, HashMap.class);
        prepareBadgeClassResponse(inputMap, badgeClassExtension, outputMap);
    }

    private static String createUri(Map<String, Object> map, String uri, int placeholderCount) {
        if (placeholderCount == 0) {
            return uri;
        } else if (placeholderCount == 1) {
            return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_ID));
        } else if (placeholderCount == 2) {
            return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_ID),
                (String) map.get(BadgingJsonKey.BADGE_CLASS_ID));
        } else {
            return MessageFormat.format(uri, (String) map.get(BadgingJsonKey.ISSUER_ID),
                (String) map.get(BadgingJsonKey.BADGE_CLASS_ID),(String) map.get(BadgingJsonKey.ASSERTION_ID));
        }
    }

    /**
     * This method will create url for badger server call.
     * in badger url pattern is first placeholder is issuerSlug, 2nd badgeclass slug
     * and 3rd badgeassertion slug
     * @param map  Map<String, Object>
     * @param uri String
     * @param placeholderCount int
     * @return Stirng url
     */
    public static String createBadgerUrl(Map<String, Object> map, String uri, int placeholderCount) {
        String url = PropertiesCache.getInstance().getProperty("sunbird_badger_baseurl")
            + createUri(map, uri, placeholderCount);
        return url + "?format=json";
    }

    /**
     * This method will create assertion request data from requested map.
     *
     * @param map
     *            Map<String,Object>
     * @return String
     */
    public static String createAssertionReqData(Map<String, Object> map) {
        JsonObject json = new JsonObject();
        json.addProperty(BadgingJsonKey.RECIPIENT_IDENTIFIER, (String) map.get(BadgingJsonKey.RECIPIENT_EMAIL));
        if(!ProjectUtil.isStringNullOREmpty((String) map.get(BadgingJsonKey.EVIDENCE))) {
            json.addProperty(BadgingJsonKey.EVIDENCE, (String) map.get(BadgingJsonKey.EVIDENCE));
        }
        json.addProperty(BadgingJsonKey.CREATE_NOTIFICATION, false);
        return json.toString();
    }

    public static void prepareBadgeIssuerResponse(String inputJson, Map<String, Object> outputMap) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String , Object> inputMap  = mapper.readValue(inputJson, HashMap.class);
        prepareBadgeIssuerResponse(inputMap, outputMap);
    }

    public static void prepareBadgeIssuerResponse(Map<String, Object> inputMap, Map<String, Object> outputMap) throws IOException {
        MapperUtil.put(inputMap, BadgingJsonKey.CREATED_AT, outputMap, JsonKey.CREATED_DATE);
        MapperUtil.put(inputMap, BadgingJsonKey.SLUG, outputMap, BadgingJsonKey.ISSUER_ID);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_ID, outputMap, BadgingJsonKey.ISSUER_ID_URL);
        MapperUtil.put(inputMap,BadgingJsonKey.JSON_URL, outputMap, BadgingJsonKey.ISSUER_URL);
        MapperUtil.put(inputMap, JsonKey.NAME, outputMap, JsonKey.NAME);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_DESCRIPTION, outputMap, JsonKey.DESCRIPTION);
        MapperUtil.put(inputMap, JsonKey.IMAGE, outputMap, JsonKey.IMAGE);
        MapperUtil.put(inputMap, BadgingJsonKey.JSON_EMAIL, outputMap, JsonKey.EMAIL);
    }

}
