package org.sunbird.learner.util;

import akka.dispatch.ExecutionContexts;
import akka.dispatch.Mapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.RestUtil;
import scala.concurrent.Future;

/** @author Mahesh Kumar Gangula */
public class ContentSearchUtil {

  private static String contentSearchURL = null;

  static {
    String baseUrl = System.getenv(JsonKey.SUNBIRD_API_MGR_BASE_URL);
    String searchPath = System.getenv(JsonKey.SUNBIRD_CS_SEARCH_PATH);
    if (StringUtils.isBlank(searchPath))
      searchPath = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CS_SEARCH_PATH);
    contentSearchURL = baseUrl + searchPath;
  }

  private static Map<String, String> getUpdatedHeaders(Map<String, String> headers) {
    if (headers == null) {
      headers = new HashMap<>();
    }
    headers.put(
        HttpHeaders.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.SUNBIRD_AUTHORIZATION));
    headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    return headers;
  }

  public static Future<Map<String, Object>> searchContent(String body, Map<String, String> headers)
      throws Exception {
    Unirest.clearDefaultHeaders();
    BaseRequest request =
        Unirest.post(contentSearchURL).headers(getUpdatedHeaders(headers)).body(body);
    Future<HttpResponse<JsonNode>> response = RestUtil.executeAsync(request);

    return commonResponseMap(response);
  }

  public static Future<Map<String, Object>> searchContent(
      String body, String queryString, Map<String, String> headers) throws Exception {
    if (StringUtils.isNotBlank(queryString)) {
      Unirest.clearDefaultHeaders();
      BaseRequest request =
          Unirest.post(contentSearchURL + queryString)
              .headers(getUpdatedHeaders(headers))
              .body(body);
      Future<HttpResponse<JsonNode>> response = RestUtil.executeAsync(request);

      return commonResponseMap(response);
    } else {
      return searchContent(body, headers);
    }
  }

  private static Future<Map<String, Object>> commonResponseMap(
      Future<HttpResponse<JsonNode>> response) throws Exception {
    return response.map(
        new Mapper<HttpResponse<JsonNode>, Map<String, Object>>() {
          @Override
          public Map<String, Object> apply(HttpResponse<JsonNode> response) {
            try {
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
                //    	    				throw new ProjectCommonException(err, message,
                // ResponseCode.SERVER_ERROR.getResponseCode());
                return null;
              }
            } catch (Exception e) {
              return null;
            }
          }
        },
        ExecutionContexts.global());
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
