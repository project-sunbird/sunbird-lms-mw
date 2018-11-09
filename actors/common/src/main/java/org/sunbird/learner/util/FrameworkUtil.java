package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.user.User;

public class FrameworkUtil {

  public static String getFrameworkReadUrl = null;
  public static String getChannelUrl = null;

  static {
    String baseUrl = System.getenv(JsonKey.SUNBIRD_WEB_URL);
    String searchPath = System.getenv(JsonKey.SUNBIRD_CHANNEL_API);
    if (StringUtils.isBlank(searchPath))
      searchPath = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CHANNEL_API);
    getFrameworkReadUrl = baseUrl + searchPath;
    getChannelUrl = baseUrl + searchPath;
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
  public static Map<String, Object> getFrameworkDeatils(String frameworkId) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> resMap = new HashMap<>();
    String response = "";
    JSONObject jObject;
    Map<String, String> headers = getHeaders(null);
    ProjectLogger.log(
        "making call to read framework Details ==" + frameworkId, LoggerEnum.INFO.name());
    try {
      response = HttpUtil.sendGetRequest(getFrameworkReadUrl + "/" + frameworkId, headers);
      ProjectLogger.log("framework Read details are ==" + response, LoggerEnum.INFO.name());
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

  public static void validateFrameworkUpdateRequest(
      User user,
      Map<String, Object> userMap,
      List<String> frameworkFields,
      List<String> frameworkMandatoryFields) {
    String frameworkId;
    if (DataCacheHandler.getchannelFrameworkMap().get(user.getChannel()) != null) {
      frameworkId = DataCacheHandler.getchannelFrameworkMap().get(user.getChannel());
    } else {
      Map<String, Object> response = ChannelUtil.getRootOrgDetails(user.getRootOrgId());
      frameworkId = (String) response.get(JsonKey.DEFAULT_FRAMEWORK);
    }
    Map<String, List<String>> frameworkRequest =
        (Map<String, List<String>>) userMap.get(JsonKey.FRAMEWORK);
    if (DataCacheHandler.getFrameworkMap().get(frameworkId) == null) {
      getFrameworkDataAndCache(frameworkId);
    }
    validateFrameworkFromCache(frameworkRequest, frameworkFields, frameworkId);
  }

  public static void getFrameworkDataAndCache(String frameworkId) {
    Map<String, Object> response = FrameworkUtil.getFrameworkDeatils(frameworkId);
    Map<String, List<Map<String, String>>> frameworkCacheMap = new HashMap<>();
    List<String> supportedfFields = DataCacheHandler.getFrameworkFieldsConfig().get(JsonKey.FIELDS);
    Map<String, Object> result = (Map<String, Object>) response.get(JsonKey.RESULT);
    Map<String, Object> frameworkDetails = (Map<String, Object>) result.get(JsonKey.FRAMEWORK);
    List<Map<String, Object>> frameworkCategories =
        (List<Map<String, Object>>) frameworkDetails.get(JsonKey.CATEGORIES);
    for (Map<String, Object> frameworkCategoriesValue : frameworkCategories) {
      String frameworkField = (String) frameworkCategoriesValue.get(JsonKey.CODE);
      List<Map<String, String>> listOfFields = new ArrayList<>();
      if (supportedfFields.contains(frameworkField)
          || frameworkField.equalsIgnoreCase(JsonKey.GRADE_LEVEL)) {
        if (frameworkField.equalsIgnoreCase(JsonKey.GRADE_LEVEL)) frameworkField = JsonKey.GRADE;
        List<Map<String, Object>> frameworkTerms =
            (List<Map<String, Object>>) frameworkCategoriesValue.get(JsonKey.TERMS);
        for (Map<String, Object> frameworkTermsField : frameworkTerms) {
          String id = (String) frameworkTermsField.get(JsonKey.IDENTIFIER);
          String name = (String) frameworkTermsField.get(JsonKey.NAME);
          Map<String, String> writtenValue = new HashMap<>();
          writtenValue.put(JsonKey.ID, id);
          writtenValue.put(JsonKey.NAME, name);
          listOfFields.add(writtenValue);
        }
      }
      if (!StringUtils.isEmpty(frameworkField) && listOfFields != null)
        frameworkCacheMap.put(frameworkField, listOfFields);
    }
    if (!frameworkCacheMap.isEmpty())
      DataCacheHandler.updateFrameworkMap(frameworkId, frameworkCacheMap);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getChannelDeatils(String channel) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> resMap = new HashMap<>();
    String response = "";
    JSONObject jObject;
    Map<String, String> headers = getHeaders(null);
    ProjectLogger.log("making call to read channel Details ==" + channel, LoggerEnum.INFO.name());
    try {
      response = HttpUtil.sendGetRequest(getChannelUrl + "/" + channel, headers);
      ProjectLogger.log("channel Read details are ==" + response, LoggerEnum.INFO.name());
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

  public static void validateFrameworkFromCache(
      Map<String, List<String>> frameworkRequest,
      List<String> frameworkFields,
      String frameworkId) {
    Map<String, List<Map<String, String>>> frameworkCachedValues =
        DataCacheHandler.getFrameworkMap().get(frameworkId);
    for (Map.Entry<String, List<String>> entry : frameworkRequest.entrySet()) {
      {
        List<Map<String, String>> cachedFrameworkList = frameworkCachedValues.get(entry.getKey());
        for (String userFieldValues : entry.getValue()) {
          boolean found = false;
          for (int i = 0; i < cachedFrameworkList.size(); i++) {
            if (cachedFrameworkList.get(i).get(JsonKey.NAME).equalsIgnoreCase(userFieldValues)) {
              found = true;
              break;
            }
          }
          if (!found)
            throw new ProjectCommonException(
                ResponseCode.errorInvalidValueProvided.getErrorCode(),
                ResponseCode.errorInvalidValueProvided.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode(),
                StringFormatter.joinByDot(JsonKey.FRAMEWORK, entry.getKey()));
        }
      }
    }
  }

  public static boolean validateFrameworkByChannel(Map<String, Object> framework, String channel) {
    ProjectLogger.log(String.format("channel : %1$s and framework %2$s", channel, framework));
    if (!preValidateRequestFramework(framework)) {
      return false;
    }
    String frameworkId = DataCacheHandler.getchannelFrameworkMap().get(channel);
    if (frameworkId == null) {
      Map<String, Object> existingChannel = getChannelDeatils(channel);
      if (existingChannel == null || existingChannel.containsKey(JsonKey.CHANNEL)) {}

      Map<String, Object> channelDetails =
          (Map<String, Object>) existingChannel.get(JsonKey.CHANNEL);
      Map<String, Object> frameworkDetails =
          (Map<String, Object>) channelDetails.get(JsonKey.FRAMEWORK);
      frameworkId = (String) frameworkDetails.get(JsonKey.IDENTIFIER);
      DataCacheHandler.upsertFrameworkToChannel(channel, frameworkId);
    }

    Map<String, Object> existingFramework = getFrameworkDeatils(frameworkId);

    return validateFrameworks(
        (Map<String, Object>) framework.get(JsonKey.FRAMEWORK),
        (Map<String, Object>) existingFramework.get(JsonKey.RESULT));
  }

  private static boolean preValidateRequestFramework(Map<String, Object> framework) {
    List<String> mandatoryFields =
        DataCacheHandler.getFrameworkFieldsConfig().get(JsonKey.MANDATORY_FIELDS);
    Map<String, Object> frameworkDetails = (Map<String, Object>) framework.get(JsonKey.FRAMEWORK);
    if (!frameworkDetails.keySet().containsAll(mandatoryFields)) {
      ProjectLogger.log("framework does not contain mandatory fields", LoggerEnum.ERROR);
      return false;
    }
    boolean isAnyFieldNotListOrEmpty =
        frameworkDetails
            .entrySet()
            .stream()
            .anyMatch(
                entry ->
                    entry.getValue() == null
                        || !(entry.getValue() instanceof List)
                        || ((List) entry.getValue()).isEmpty());
    if (isAnyFieldNotListOrEmpty) {
      ProjectLogger.log(
          "framework fields can only by list and cannot be null or empty", LoggerEnum.ERROR);
    }
    return true;
  }

  private static boolean validateFrameworks(
      Map<String, Object> framework, Map<String, Object> existing) {
    Map<String, Object> existingFramework =
        (Map<String, Object>) existingFramework.get(JsonKey.FRAMEWORK);
    List<Map<String, Object>> categories =
        (List<Map<String, Object>>) existingFramework.get(JsonKey.CATEGORIES);
    final Map<String, List<Map<String, Object>>> categoriesTermsByCode = new HashMap<>();
    categories.forEach(
        category ->
            categoriesTermsByCode.put(
                (String) category.get(JsonKey.CODE),
                (List<Map<String, Object>>) category.get(JsonKey.TERMS)));

    for (Entry<String, Object> entry : framework.entrySet()) {
      boolean valid =
          validateFrameWorkList(entry.getKey(), entry.getValue(), categoriesTermsByCode);
      if (!valid) {
        return false;
      }
    }

    return true;
  }

  private static boolean validateFrameWorkList(
      String key, Object value, Map<String, List<Map<String, Object>>> categoriesTermsByCode) {

    List<String> existingCodes =
        categoriesTermsByCode
            .get(key)
            .stream()
            .map(term -> (String) term.get(JsonKey.CODE))
            .collect(Collectors.toList());
    List<String> inputList = (List<String>) value;
    if (inputList.stream().anyMatch(input -> !existingCodes.contains(input))) {
      ProjectLogger.log(
          String.format("mismatch : %1$s : %2$s and existing %3$s", key, inputList, existingCodes),
          LoggerEnum.ERROR);
      return false;
    }
    return true;
  }
}
