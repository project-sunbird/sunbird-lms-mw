package org.sunbird.badge.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.sunbird.badge.model.BadgeClassExtension;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.MapperUtil;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;

public class BadgingUtil {

    public static final String SUNBIRD_BADGR_SERVER_URL_DEFAULT = "http://localhost:8000";
    public static final String BADGING_AUTHORIZATION_FORMAT = "Token %s";
    public static final String SUNBIRD_BADGER_CREATE_ASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertions";
    public static final String SUNBIRD_BADGER_GETASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertions/{2}";
    public static final String SUNBIRD_BADGER_GETALLASSERTION_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion";
    public static final String SUNBIRD_BADGER_REVOKE_URL="/v1/issuer/issuers/{0}/badges/{1}/assertion/{2}";

    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private static BadgingService service = BadgingFactory.getInstance();

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
        headers.put("Content-Type", "application/json");
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
            return MessageFormat.format(uri, map.get(BadgingJsonKey.ISSUER_ID), map.get(BadgingJsonKey.BADGE_ID));
        } else {
            return MessageFormat.format(uri, map.get(BadgingJsonKey.ISSUER_ID),
                map.get(BadgingJsonKey.BADGE_ID), map.get(BadgingJsonKey.ASSERTION_ID));
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

    /**
     * This method will create assertion revoke request data.
     *
     * @param map
     *            Map<String,Object>
     * @return String
     */
	public static String createAssertionRevokeData(Map<String, Object> map) {
		JsonObject json = new JsonObject();
		json.addProperty("revocation_reason", (String) map.get(BadgingJsonKey.REVOCATION_REASON));
		return json.toString();
	}
	
	/**
	 * This method will create project common exception object for 
	 * 400, 401, 403 , 404 and rest all will come under 500 server error.
	 * need to call this method incase of error only. 
	 * @param statusCode int
	 * @return ProjectCommonException
	 */
	public static ProjectCommonException createExceptionForBadger(int statusCode) {
		ProjectLogger.log("Badger is sending status code as " + statusCode, LoggerEnum.INFO.name());
		switch (statusCode) {
		case 400:
			return new ProjectCommonException(ResponseCode.invalidData.getErrorCode(),
					ResponseCode.invalidData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		case 401:
			return new ProjectCommonException(ResponseCode.unAuthorised.getErrorCode(),
					ResponseCode.unAuthorised.getErrorMessage(), ResponseCode.UNAUTHORIZED.getResponseCode());
		case 403:
			return new ProjectCommonException(ResponseCode.unAuthorised.getErrorCode(),
					ResponseCode.unAuthorised.getErrorMessage(), ResponseCode.UNAUTHORIZED.getResponseCode());
		case 404:
			return new ProjectCommonException(ResponseCode.resourceNotFound.getErrorCode(),
					ResponseCode.resourceNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
		default:
			return new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
					ResponseCode.internalError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		}

	}
	
	/**
	 * This method will get the response from badging server and 
	 * create proper map (meta data) to store either with user or content.
	 * @param reqMap  Map<String,Object>
	 * @return Map<String,Object>
	 */
	public static Map<String, Object> createBadgeNotifierMap(Map<String, Object> reqMap) {
		Map<String, Object> outerMap = new HashMap<>();
		Map<String, Object> innerMap = new HashMap<>();
		innerMap.put(BadgingJsonKey.ASSERTION_ID, reqMap.get(BadgingJsonKey.ASSERTION_ID));
		innerMap.put(BadgingJsonKey.BADGE_CLASS_ID, reqMap.get(BadgingJsonKey.BADGE_CLASS_ID));
		innerMap.put(BadgingJsonKey.ISSUER_ID, reqMap.get(BadgingJsonKey.ISSUER_ID));
		innerMap.put(BadgingJsonKey.BADGE_CLASS_IMAGE, reqMap.get(BadgingJsonKey.BADGE_ID_URL));
		innerMap.put(JsonKey.STATUS, BadgeStatus.active.name());
		innerMap.put(BadgingJsonKey.CREATED_TS, System.currentTimeMillis());
		// now make a badgr call to collect badgeClassName
		Request request = new Request();
		request.getRequest().put(BadgingJsonKey.ISSUER_ID, reqMap.get(BadgingJsonKey.ISSUER_ID));
		request.getRequest().put(BadgingJsonKey.BADGE_CLASS_ID, reqMap.get(BadgingJsonKey.BADGE_CLASS_ID));
		try {
			Response response = service.getBadgeClassDetails(request);
			request.getRequest().put(BadgingJsonKey.BADGE_CLASS_NANE, response.getResult().get(JsonKey.NAME));
		} catch (IOException | ProjectCommonException e) {
			ProjectLogger.log(e.getMessage(), e);
		}
		outerMap.put(BadgingJsonKey.BADGE_ASSERTION, innerMap);
		return outerMap;
	}
	
	/**
	 * This method will take response data from badger server as input map and then prepare a
	 * new map as outPut map by changing the key and some values.
	 * @param inputMap Map<String,Object>
	 * @param outPutMap Map<String,Object>
	 * @return Map<String,Object>
	 */
	public static Map<String, Object> prepareAssertionResponse(Map<String, Object> inputMap,
			Map<String, Object> outPutMap) {
		MapperUtil.put(inputMap, BadgingJsonKey.CREATED_AT, outPutMap, JsonKey.CREATED_DATE);
		MapperUtil.put(inputMap, BadgingJsonKey.JSON_ISSUED_ON, outPutMap, BadgingJsonKey.ASSERTION_DATE);
		MapperUtil.put(inputMap, BadgingJsonKey.SLUG, outPutMap, BadgingJsonKey.ASSERTION_ID);
		MapperUtil.put(inputMap, BadgingJsonKey.JSON_ID, outPutMap, BadgingJsonKey.ASSERTION_ID_URL);
		MapperUtil.put(inputMap, BadgingJsonKey.JSON_IMAGE, outPutMap, BadgingJsonKey.ASSERTION_IMAGE_URL);
		MapperUtil.put(inputMap, BadgingJsonKey.BADGE_CLASS, outPutMap, BadgingJsonKey.BADGE_ID_URL);
		if (outPutMap.containsKey(BadgingJsonKey.BADGE_ID_URL)) {
			outPutMap.put(BadgingJsonKey.BADGE_ID, getLastSegment((String) outPutMap.get(BadgingJsonKey.BADGE_ID_URL)));
		}
		MapperUtil.put(inputMap, BadgingJsonKey.ISSUER, outPutMap, BadgingJsonKey.ISSUER_ID_URL);
		if (outPutMap.containsKey(BadgingJsonKey.ISSUER_ID_URL)) {
			outPutMap.put(BadgingJsonKey.ISSUER_ID,
					getLastSegment((String) outPutMap.get(BadgingJsonKey.ISSUER_ID_URL)));
		}
		MapperUtil.put(inputMap, BadgingJsonKey.JSON_RECIPIENT, outPutMap, BadgingJsonKey.RECIPIENT);
		MapperUtil.put(inputMap, BadgingJsonKey.RECIPIENT_IDENTIFIER, outPutMap, BadgingJsonKey.RECIPIENT_EMAIL);
		MapperUtil.put(inputMap, BadgingJsonKey.JSON_VERIFY, outPutMap, BadgingJsonKey.VERIFY);
		MapperUtil.put(inputMap, BadgingJsonKey.REVOKED, outPutMap, BadgingJsonKey.REVOKED);
		MapperUtil.put(inputMap, BadgingJsonKey.REVOCATION_REASON_BADGE, outPutMap, BadgingJsonKey.REVOCATION_REASON);
		return outPutMap;
	}
    
	
	/**
	 * This method will get the response from badging server and 
	 * create proper map (meta data) to remove badge assertion either from user or content.
	 * @param reqMap  Map<String,Object>
	 * @return Map<String,Object>
	 */
	public static Map<String, Object> createRevokeBadgeNotifierMap(Map<String, Object> reqMap) {
		Map<String, Object> outerMap = new HashMap<>();
		Map<String, Object> innerMap = new HashMap<>();
		innerMap.put(BadgingJsonKey.ASSERTION_ID, reqMap.get(BadgingJsonKey.ASSERTION_ID));
		innerMap.put(BadgingJsonKey.BADGE_CLASS_ID, reqMap.get(BadgingJsonKey.BADGE_CLASS_ID));
		innerMap.put(BadgingJsonKey.ISSUER_ID, reqMap.get(BadgingJsonKey.ISSUER_ID));
		outerMap.put(BadgingJsonKey.BADGE_ASSERTION, innerMap);
		return outerMap;
	}
	
	public enum BadgeStatus {
		active,revoked;
	}
}
