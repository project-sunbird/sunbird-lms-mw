package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.RestUtil;
import org.sunbird.common.responsecode.ResponseCode;

/** @author Mahesh Kumar Gangula */
public class ContentSearchUtil {

  private static ObjectMapper mapper = new ObjectMapper();
  private static String contentSearchURL = null;
  private static HashMap<String, String> headers = new HashMap<String, String>();

  static {
    String baseUrl = System.getenv(JsonKey.SUNBIRD_API_MGR_BASE_URL);
    String searchPath = System.getenv(JsonKey.SUNBIRD_CS_SEARCH_PATH);
    if (StringUtils.isBlank(searchPath))
      searchPath = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CS_SEARCH_PATH);
    contentSearchURL = baseUrl + searchPath;
    headers.put("Content-Type", "application/json");
    headers.put(
        JsonKey.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.SUNBIRD_AUTHORIZATION));
  }

  public static Map<String, Object> searchContent(String body) throws Exception {
    String httpResponse = HttpUtil.sendPostRequest(contentSearchURL, body, headers);
    JSONObject jObject = new JSONObject(httpResponse);
    String resmsgId = (String) jObject.getJSONObject("params").get("resmsgid");
    String apiId = jObject.getString("id");
    String resultStr = jObject.getString(JsonKey.RESULT);
    Map<String, Object> data = mapper.readValue(resultStr, Map.class);
    Object contentList = data.get(JsonKey.CONTENT);
    Map<String, Object> response = new HashMap<>();
    Map<String, Object> param = new HashMap<>();
    param.put(JsonKey.RES_MSG_ID, resmsgId);
    param.put(JsonKey.API_ID, apiId);
    response.put(JsonKey.PARAMS, param);
    response.put(JsonKey.CONTENTS, contentList);
    Iterator<Map.Entry<String, Object>> itr = data.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<String, Object> entry = itr.next();
      if (!JsonKey.CONTENT.equals(entry.getKey())) {
        response.put(entry.getKey(), entry.getValue());
      }
    }
    return response;
  }

  public static Map<String, Object> searchContentUsingUnirest(String body) throws Exception {
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
      } else if (value instanceof JSONObject) {
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
      } else if (value instanceof JSONObject) {
        value = jsonToMap((JSONObject) value);
      }
      list.add(value);
    }
    return list;
  }
}
