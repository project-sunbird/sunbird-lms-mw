package org.sunbird.content.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ContentService {

	private static final String contentServiceBaseUrl = System.getenv(JsonKey.SUNBIRD_API_BASE_URL);
	private static ObjectMapper mapper = new ObjectMapper();
	private static Map<String, String> headers = new HashMap<String, String>();
	private static final String BADGE_ASSERTION = "badgeAssertion";

	static {
		String authorization = System.getenv(JsonKey.SUNBIRD_AUTHORIZATION);
		if (ProjectUtil.isStringNullOREmpty(authorization)) {
			authorization = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_AUTHORIZATION);
		} else {
			authorization = JsonKey.BEARER + authorization;
		}
		headers.put(JsonKey.AUTHORIZATION, authorization);
		headers.put("Content-Type", "application/json");
	}

	public static Response assignBadge(Request request) throws Exception {
		return processBadge(request, "ASSIGNBADGE");
	}

	public static Response revokeBadge(Request request) throws Exception {
		return processBadge(request, "REVOKEBADGE");
	}

	@SuppressWarnings("unchecked")
	private static Response processBadge(Request request, String operation) throws Exception {
		String id = (String) request.getRequest().get("id");
		Map<String, Object> badge = (Map<String, Object>) request.getRequest().get(BADGE_ASSERTION);
		Map<String, String> props = getProperties(operation);
		if (StringUtils.isBlank(id)) {
			throw new ProjectCommonException(props.get("errCode"), "Please provide content id.",
					ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		if (null == badge || badge.isEmpty()) {
			throw new ProjectCommonException(props.get("errCode"), "Please provide badge details.",
					ResponseCode.CLIENT_ERROR.getResponseCode());
		}

		String badgeStr = mapper.writeValueAsString(badge);
		String reqBody = "{\"request\": {\"content\": {\"" + BADGE_ASSERTION + "\": " + badgeStr + "}}}";

		HttpUtil.sendPatchRequest(props.get("basePath") + id, reqBody, headers);
		// TODO: Get the response and return msg based on it's value.
		Response response = new Response();
		response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		return response;
	}

	private static Map<String, String> getProperties(String operation) {
		Map<String, String> props = new HashMap<String, String>();
		switch (operation.toUpperCase()) {
		case "ASSIGNBADGE":
			props.put("basePath",
					contentServiceBaseUrl + PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CONTENT_BADGE_ASSIGN_URL));
			props.put("errCode", "INVALID_ASSIGN_BADGE_REQUEST");
			break;
		case "REVOKEBADGE":
			props.put("basePath",
					contentServiceBaseUrl + PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CONTENT_BADGE_REVOKE_URL));
			props.put("errCode", "INVALID_REVOKE_BADGE_REQUEST");
			break;
		default:
			break;
		}
		return props;
	}

}
