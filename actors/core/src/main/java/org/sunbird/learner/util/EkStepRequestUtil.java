/** */
package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;

/**
 * This class will make the call to EkStep content search
 *
 * @author Manzarul
 */
public final class EkStepRequestUtil {

  private static ObjectMapper mapper = new ObjectMapper();

  private EkStepRequestUtil() {}

  /**
   * @param params String
   * @param headers Map<String, String>
   * @return Map<String,Object>
   */
  public static Map<String, Object> searchContent(String params, Map<String, String> headers) {
    Object[] result = null;
    Map<String, Object> resMap = new HashMap<>();
    String response = "";

    JSONObject jObject;
    try {
      String baseSearchUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
      if (StringUtils.isBlank(baseSearchUrl)) {
        baseSearchUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL);
      }
      headers.put(
          JsonKey.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.EKSTEP_AUTHORIZATION));
      headers.put("Content-Type", "application/json");
      if (StringUtils.isBlank(headers.get(JsonKey.AUTHORIZATION))) {
        headers.put(
            JsonKey.AUTHORIZATION,
            PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
      }
      ProjectLogger.log("making call for content search ==" + params, LoggerEnum.INFO.name());
      response =
          HttpUtil.sendPostRequest(
              baseSearchUrl
                  + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_URL),
              params,
              headers);
      ProjectLogger.log("Content serach response is ==" + response, LoggerEnum.INFO.name());
      jObject = new JSONObject(response);
      String resmsgId = (String) jObject.getJSONObject("params").get("resmsgid");
      String apiId = jObject.getString("id");
      String resultStr = jObject.getString(JsonKey.RESULT);
      Map<String, Object> data = mapper.readValue(resultStr, Map.class);
      ProjectLogger.log(
          "Total number of content fetched from Ekstep while assembling page data : "
              + data.get("count"),
          LoggerEnum.INFO.name());
      Object contentList = data.get(JsonKey.CONTENT);
      Map<String, Object> param = new HashMap<>();
      param.put(JsonKey.RES_MSG_ID, resmsgId);
      param.put(JsonKey.API_ID, apiId);
      resMap.put(JsonKey.PARAMS, param);
      resMap.put(JsonKey.CONTENTS, contentList);
      Iterator<Map.Entry<String, Object>> itr = data.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry<String, Object> entry = itr.next();
        if (!JsonKey.CONTENT.equals(entry.getKey())) {
          resMap.put(entry.getKey(), entry.getValue());
        }
      }
    } catch (IOException | JSONException e) {
      ProjectLogger.log("Error found during contnet search parse==" + e.getMessage(), e);
    }
    return resMap;
  }
}
