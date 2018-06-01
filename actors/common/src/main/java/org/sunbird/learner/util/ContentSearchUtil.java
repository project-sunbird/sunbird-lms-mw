package org.sunbird.learner.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.RestUtil;
import org.sunbird.common.responsecode.ResponseCode;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class ContentSearchUtil {

	private static String contentSearchURL = null;
	private static HashMap<String, String> headers = new HashMap<String, String>();

	static {
		String baseUrl = System.getenv(JsonKey.SUNBIRD_API_BASE_URL);
		String compositeSearch = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_COMPOSITE_SEARCH_URL);
		contentSearchURL = baseUrl + compositeSearch;
		headers.put("Content-Type", "application/json");
		headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.SUNBIRD_AUTHORIZATION));
	}

	public static Map<String, Object> searchContent(String body) throws Exception {
		Unirest.clearDefaultHeaders();
		BaseRequest request = Unirest.post(contentSearchURL).headers(headers).body(body);
		HttpResponse<JsonNode> response = RestUtil.execute(request);
		if (RestUtil.isSuccessful(response)) {
			JSONObject result = response.getBody().getObject().getJSONObject("result");
			Map<String, Object> resultMap = jsonToMap(result);
			Object contents = resultMap.get(JsonKey.CONTENT);
			resultMap.remove(JsonKey.CONTENT);
			resultMap.put(JsonKey.CONTENTS, contents);
			String resmsgId = RestUtil.getFromResponse(response, "params.resmsgid");
			String apiId = RestUtil.getFromResponse(response, "id");
			Map<String, Object> param = new HashMap<>();
			param.put(JsonKey.RES_MSG_ID, resmsgId);
			param.put(JsonKey.API_ID, apiId);
			resultMap.put(JsonKey.PARAMS, param);
			return resultMap;
		} else {
			String err = RestUtil.getFromResponse(response, "params.err");
			String message = RestUtil.getFromResponse(response, "params.errmsg");
			throw new ProjectCommonException(err, message, ResponseCode.SERVER_ERROR.getResponseCode());
		}
	}

	public static Map<String, Object> jsonToMap(JSONObject object) throws JSONException {
		Map<String, Object> map = new HashMap<String, Object>();

		Iterator<String> keysItr = object.keys();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = jsonToMap((JSONObject) value);
			}
			map.put(key, value);
		}
		return map;
	}

	public static List<Object> toList(JSONArray array) throws JSONException {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); i++) {
			Object value = array.get(i);
			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = jsonToMap((JSONObject) value);
			}
			list.add(value);
		}
		return list;
	}

}
