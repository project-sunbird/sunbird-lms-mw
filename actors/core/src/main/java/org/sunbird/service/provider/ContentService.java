package org.sunbird.service.provider;

import java.util.HashMap;
import java.util.List;
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

	private static final String ekstepInfraUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
	private static ObjectMapper mapper = new ObjectMapper();
	private static String BADGE_LIST_KEY = "testBadgeList";
	private static Map<String, String> headers = new HashMap<String, String>();

	static {
		String authorization = System.getenv(JsonKey.EKSTEP_AUTHORIZATION);
		if (ProjectUtil.isStringNullOREmpty(authorization)) {
			authorization = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION);
		} else {
			authorization = JsonKey.BEARER + authorization;
		}
		headers.put(JsonKey.AUTHORIZATION, authorization);
		headers.put("Content-Type", "application/json");
	}

	@SuppressWarnings("unchecked")
	public static Response assignBadge(Request request) throws Exception {
		String id = (String) request.getRequest().get("identifier");
		List<Map<String, Object>> badge = (List<Map<String, Object>>) request.getRequest().get("badge");

		if (StringUtils.isBlank(id)) {
			throw new ProjectCommonException("INVALID_ASSIGN_BADGE_REQUEST", "Please provide content id.", ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		if (null == badge || badge.isEmpty()) {
			throw new ProjectCommonException("INVALID_ASSIGN_BADGE_REQUEST", "Please provide badge details.", ResponseCode.CLIENT_ERROR.getResponseCode());
		}

		String url = ekstepInfraUrl + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_UPDATE_URL) + id;
		String badgeStr = mapper.writeValueAsString(badge);
		String reqBody = "{\"request\": {\"content\": {\"" + BADGE_LIST_KEY + "\": " + badgeStr + "}}}";
		String httpResponse = HttpUtil.sendPatchRequest(url, reqBody, headers);
		System.out.println("Response: " + httpResponse);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		return response;
	}

}
