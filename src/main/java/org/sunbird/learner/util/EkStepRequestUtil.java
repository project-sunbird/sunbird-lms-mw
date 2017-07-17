package org.sunbird.learner.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EkStepRequestUtil {

	private static ObjectMapper mapper = new ObjectMapper();
	private static Map<String, String> headers = new HashMap<String, String>();
	static {
		headers.put("content-type", "application/json");
		headers.put("accept", "application/json");
	}
	
	public static Object[] searchContent(String params) {
		Object[] result = null;
		String response = "";
		JSONObject data;
		JSONObject jObject;
		try {
			response = HttpUtil.sendPostRequest(
					PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_SEARCH_URL), params, headers);
			jObject = new JSONObject(response);
			data = jObject.getJSONObject(JsonKey.RESULT);
			JSONArray contentArray = data.getJSONArray(JsonKey.CONTENT);
			result = mapper.readValue(contentArray.toString(), Object[].class);
		} catch (IOException e) {
			ProjectLogger.log(e.getMessage(), e);
		} catch (JSONException e) {
		    ProjectLogger.log(e.getMessage(), e);
		}
		return result;
	}
}
