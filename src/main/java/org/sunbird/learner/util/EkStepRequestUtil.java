package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;

public class EkStepRequestUtil {

	private static LogHelper logger = LogHelper.getInstance(EkStepRequestUtil.class.getName());
	private static ObjectMapper mapper = new ObjectMapper();
	
	public static Object[] searchContent(String params, Map<String, String> headers) {
		Object[] result = null;
		String response = "";
		JSONObject data;
		JSONObject jObject;
		try {
		  String baseSearchUrl = System.getenv(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
		  if(ProjectUtil.isStringNullOREmpty(baseSearchUrl)){
		    baseSearchUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
		  }
			response = HttpUtil.sendPostRequest(baseSearchUrl+
					PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_SEARCH_URL), params, headers);
			jObject = new JSONObject(response);
			data = jObject.getJSONObject(JsonKey.RESULT);
			JSONArray contentArray = data.getJSONArray(JsonKey.CONTENT);
			result = mapper.readValue(contentArray.toString(), Object[].class);
		} catch (IOException | JSONException e) {
			logger.error(e.getMessage(), e);
			ProjectLogger.log(e.getMessage(), e);
		} 
		return result;
	}
}
