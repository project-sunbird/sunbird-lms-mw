package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;

public class FrameworkUtil {

  public static String getRootOrgDetailsUrl = null;

  static {
    String baseUrl = System.getenv(JsonKey.SUNBIRD_WEB_URL);
    String searchPath = System.getenv(JsonKey.SUNBIRD_CHANNEL_API);
    if (StringUtils.isBlank(searchPath))
      searchPath = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CHANNEL_API);
    getRootOrgDetailsUrl = baseUrl + searchPath;
  }

  private static Map<String, String> getHeaders(Map<String, String> headers) {
    if (headers == null) {
      headers = new HashMap<>();
    }
    headers.put(
        HttpHeaders.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.SUNBIRD_AUTHORIZATION));
    return headers;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getRootOrgDetails(String rootOrgId) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> resMap = new HashMap<>();
    String response = "";
    JSONObject jObject;
    Map<String, String> headers = getHeaders(null);
    ProjectLogger.log("making call to read RootOrg Details ==" + rootOrgId, LoggerEnum.INFO.name());
    try {
      response = HttpUtil.sendGetRequest(getRootOrgDetailsUrl + "/" + rootOrgId, headers);
      ProjectLogger.log("RootOrg Read details are ==" + response, LoggerEnum.INFO.name());
      jObject = new JSONObject(response);
      String apiId = jObject.getString("id");
      String resmsgId = (String) jObject.getJSONObject("params").get("resmsgid");
      String status = (String) jObject.getJSONObject(JsonKey.PARAMS).get(JsonKey.STATUS);
      String resultStr = jObject.getString(JsonKey.RESULT);
      Map<String, Object> data = mapper.readValue(resultStr, Map.class);
      Map<String, Object> param = new HashMap<>();
      param.put(JsonKey.RES_MSG_ID, resmsgId);
      param.put(JsonKey.ID, apiId);
      param.put(JsonKey.STATUS, status);
      resMap.put(JsonKey.PARAMS, param);
      resMap.put(JsonKey.RESULT, data);
    } catch (IOException | JSONException e) {
      ProjectLogger.log("Error found during contnet search parse==" + e.getMessage(), e);
    }
    return resMap;
  }
}
