package org.sunbird.metrics.actors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

public abstract class BaseMetricsActor extends UntypedAbstractActor {
  
  private static ObjectMapper mapper = new ObjectMapper();

  protected abstract Map<String, Object> getViewData(String id, Object data);
  public static final String startDate = "startDate";
  public static final String endDate = "endDate";
  public static final String startTimeMilis = "startTimeMilis";
  public static final String endTimeMilis = "endTimeMilis";
  public static final String LTE = "<=";
  public static final String LT = "<";
  public static final String GTE = ">=";
  public static final String GT = ">";
  public static final String KEY = "key";
  public static final String KEYNAME = "key_name";
  public static final String GROUP_ID = "group_id";
  public static final String VALUE = "value";
  public static final String INTERVAL = "interval";
  public static final String FORMAT = "format";
  

  protected Map<String, Object> addSnapshot(String keyName, String name, Object value,
      String timeUnit) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put(JsonKey.NAME, name);
    snapshot.put(VALUE, value);
    if (!ProjectUtil.isStringNullOREmpty(timeUnit)) {
      snapshot.put(JsonKey.TIME_UNIT, timeUnit);
    }
    return snapshot;
  }

  protected static Map<String, Object> getStartAndEndDate(String period) {
    Map<String, Object> dateMap = new HashMap<>();
    int days = getDaysByPeriod(period);
    Date endDateValue = new Date();
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);
    calendar.set(Calendar.HOUR_OF_DAY,23);
    calendar.set(Calendar.MINUTE, 59);
    calendar.set(Calendar.SECOND, 59);
    calendar.set(Calendar.MILLISECOND, 0);
    endDateValue = calendar.getTime();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(endDateValue.getTime());
    cal.add(Calendar.DATE, -(days-1));
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    String startDateStr = sdf.format(cal.getTimeInMillis());
    String endDateStr = sdf.format(endDateValue.getTime());
    dateMap.put(startDate, startDateStr);
    dateMap.put(endDate, endDateStr);
    dateMap.put(startTimeMilis, cal.getTimeInMillis());
    dateMap.put(endTimeMilis, endDateValue.getTime());
    return dateMap;
  }

  protected static Map<String, Object> getStartAndEndDateForWeek(String period) {
    Map<String, Object> dateMap = new HashMap<>();
    Map<String, Integer> periodMap = getDaysByPeriodStr(period);
    Date endDateValue = new Date();
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);
    calendar.set(Calendar.HOUR_OF_DAY,23);
    calendar.set(Calendar.MINUTE, 59);
    calendar.set(Calendar.SECOND, 59);
    calendar.set(Calendar.MILLISECOND, 0);
    endDateValue = calendar.getTime();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(endDateValue.getTime());
    if(Calendar.DATE == periodMap.get(KEY)){
      cal.add(periodMap.get(KEY), -(periodMap.get(VALUE)- 1));
      dateMap.put(INTERVAL, "1d");
      dateMap.put(FORMAT, "yyyy-MM-dd");
    } else { 
      cal.add(periodMap.get(KEY), -(periodMap.get(VALUE)));
      if(cal.getFirstDayOfWeek() < cal.get(Calendar.DAY_OF_WEEK)){
        cal.add(Calendar.DATE, cal.get(Calendar.DAY_OF_WEEK)+ 1);
      }
      dateMap.put(INTERVAL, "1w");
      dateMap.put(FORMAT, "yyyy-ww");
    }
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    String startDateStr = sdf.format(cal.getTimeInMillis());
    String endDateStr = sdf.format(endDateValue.getTime());
    dateMap.put(startDate, startDateStr);
    dateMap.put(endDate, endDateStr);
    dateMap.put(startTimeMilis, cal.getTimeInMillis());
    dateMap.put(endTimeMilis, endDateValue.getTime());
    return dateMap;
  }
  
  protected static int getDaysByPeriod(String period) {
    int days = 0;
    switch (period) {
      case "7d": {
        days = 7;
        break;
      }
      case "14d": {
        days = 14;
        break;
      }
      case "5w": {
        days = 35;
        break;
      }
    }
    if(days == 0){
      throw new ProjectCommonException(ResponseCode.invalidPeriod.getErrorCode(),
            ResponseCode.invalidPeriod.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return days;
  }
  
  protected static Map<String, Integer> getDaysByPeriodStr(String period) {
    Map<String, Integer> dayPeriod = new HashMap<>();
    switch (period) {
      case "7d": {
        dayPeriod.put(KEY,Calendar.DATE);
        dayPeriod.put(VALUE, 7);
        break;
      }
      case "14d": {
        dayPeriod.put(KEY,Calendar.DATE);
        dayPeriod.put(VALUE, 14);
        break;
      }
      case "5w": {
        dayPeriod.put(KEY,Calendar.WEEK_OF_YEAR);
        dayPeriod.put(VALUE, 5);
        break;
      }
    }
    if(dayPeriod.isEmpty()){
      throw new ProjectCommonException(ResponseCode.invalidPeriod.getErrorCode(),
            ResponseCode.invalidPeriod.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return dayPeriod;
  }
  
  protected static String getEkstepPeriod(String period) {
    String days = "";
    switch (period) {
      case "7d": {
        days = "LAST_7_DAYS";
        break;
      }
      case "14d": {
        days = "LAST_14_DAYS";
        break;
      }
      case "5w": {
        days = "LAST_5_WEEKS";
        break;
      }
    }
    return days;
  }
  
  protected List<Map<String,Object>> createBucketStrForWeek(String periodStr) {
    Map<String, Object> periodMap = getStartAndEndDateForWeek(periodStr);
    String date = (String) periodMap.get(startDate);
    List<Map<String,Object>> bucket = new ArrayList<>();
    Calendar cal = Calendar.getInstance();
    for(int day = 0; day < 5; day++){
      Map<String, Object> bucketData = new LinkedHashMap<String, Object>(); 
      String keyName = "";
      String key = "";
      Date dateValue = null;
      try {
         keyName = formatKeyNameString(date);
         dateValue = new SimpleDateFormat("yyyy-MM-dd").parse(date);
         cal.setTime(dateValue);
         int week = cal.get(Calendar.WEEK_OF_YEAR);
         key = cal.get(Calendar.YEAR)+ ""+ week;
         date = keyName.toLowerCase().split("to")[1];
         dateValue = new SimpleDateFormat("yyyy-MM-dd").parse(date);
         cal.setTime(dateValue);
         cal.add(Calendar.DATE, +1);
         date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
      } catch (ParseException e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      bucketData.put(KEY, key);
      bucketData.put(KEYNAME, keyName);
      bucketData.put(VALUE, 0);
      bucket.add(bucketData);
    }
    return bucket;
  }

  protected List<Map<String,Object>> createBucketStructure(String periodStr) {
    if("5w".equalsIgnoreCase(periodStr)){
      return createBucketStrForWeek(periodStr);
    }else {
      return createBucketStructureDays(periodStr);
    }
  }

  protected List<Map<String,Object>> createBucketStructureDays(String periodStr) {
    int days = getDaysByPeriod(periodStr);
    Date date = new Date();
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);
    date = calendar.getTime();
    List<Map<String,Object>> bucket = new ArrayList<>();
    for(int day = days-1; day >= 0; day--){
      Map<String, Object> bucketData = new LinkedHashMap<String, Object>();
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(date.getTime());
      cal.add(Calendar.DATE, -(day));
      bucketData.put(KEY, cal.getTimeInMillis());
      bucketData.put(KEYNAME, new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime()));
      bucketData.put(VALUE, 0);
      bucket.add(bucketData);
    }
    return bucket;
  }

  protected String getDataFromEkstep(String request, String apiUrl) {
    Map<String, String> headers = new HashMap<>();
    String response = null;
    try {
      String baseSearchUrl = System.getenv(JsonKey.EKSTEP_METRICS_URL);
      if (ProjectUtil.isStringNullOREmpty(baseSearchUrl)) {
        baseSearchUrl =
            PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_METRICS_URL);
      }
      headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
      if (ProjectUtil.isStringNullOREmpty((String) headers.get(JsonKey.AUTHORIZATION))) {
        headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER
            + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_METRICS_AUTHORIZATION));
        headers.put("Content_Type", "application/json; charset=utf-8");
      }
      response = HttpUtil.sendPostRequest(
          baseSearchUrl + PropertiesCache.getInstance().getProperty(apiUrl), request, headers);

    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return response;
  }

  public static String makePostRequest(String url, String body) throws Exception {
    String baseSearchUrl = System.getenv(JsonKey.EKSTEP_METRICS_URL);
    if (ProjectUtil.isStringNullOREmpty(baseSearchUrl)) {
      baseSearchUrl =
          PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_METRICS_URL);
    }
    String authKey = System.getenv(JsonKey.AUTHORIZATION);
    if(ProjectUtil.isStringNullOREmpty(authKey)){
      authKey = JsonKey.BEARER
          + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_METRICS_AUTHORIZATION);
    }
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(baseSearchUrl + PropertiesCache.getInstance().getProperty(url));
    post.addHeader("Content-Type", "application/json; charset=utf-8");
    post.addHeader(JsonKey.AUTHORIZATION, authKey);
    post.setEntity(new StringEntity(body, Charsets.UTF_8.name()));
    HttpResponse response = client.execute(post);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new ProjectCommonException(ResponseCode.unableToConnect.getErrorCode(),
          ResponseCode.unableToConnect.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    BufferedReader rd = new BufferedReader(
        new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8));

    StringBuffer result = new StringBuffer();
    String line = "";
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }
    return result.toString();
  }
  
  @SuppressWarnings({"rawtypes"})
  protected List<Map<String, Object>> getBucketData(Map aggKeyMap, String period) {
    if (null == aggKeyMap || aggKeyMap.isEmpty()) {
      return new ArrayList<Map<String, Object>>();
    }
    if ("5w".equalsIgnoreCase(period)) {
      return getBucketDataForWeeks(aggKeyMap);
    } else {
      return getBucketDataForDays(aggKeyMap);
    }
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected List<Map<String, Object>> getBucketDataForDays(Map aggKeyMap) {
    List<Map<String, Object>> parentGroupList = new ArrayList<Map<String, Object>>();
    List<Map<String, Double>> aggKeyList = (List<Map<String, Double>>) aggKeyMap.get("buckets");
    for (Map aggKeyListMap : aggKeyList) {
      Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
      parentCountObject.put(KEY, aggKeyListMap.get(KEY));
      parentCountObject.put(KEYNAME, aggKeyListMap.get("key_as_string"));
      parentCountObject.put(VALUE, aggKeyListMap.get("doc_count"));
      parentGroupList.add(parentCountObject);
    }
    return parentGroupList;
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected List<Map<String, Object>> getBucketDataForWeeks(Map aggKeyMap) {
    List<Map<String, Object>> parentGroupList = new ArrayList<Map<String, Object>>();
    List<Map<String, Double>> aggKeyList = (List<Map<String, Double>>) aggKeyMap.get("buckets");
    for (Map aggKeyListMap : aggKeyList) {
      Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
      parentCountObject.put(KEY, formatKeyString((String)aggKeyListMap.get("key_as_string")));
      parentCountObject.put(KEYNAME, formatKeyNameString(aggKeyListMap.get(KEY)));
      parentCountObject.put(VALUE, aggKeyListMap.get("doc_count"));
      parentGroupList.add(parentCountObject);
    }
    return parentGroupList;
  }
  
  protected String formatKeyString(String key){
    return StringUtils.remove(key, "-");
  }
  
  protected String formatKeyNameString(Object keyName) {
    StringBuffer buffer = new StringBuffer();
    Date date = new Date();
    if (keyName instanceof Long) {
      date = new Date((Long) keyName);
    } else if (keyName instanceof String) {
      try {
        date = new SimpleDateFormat("yyyy-MM-dd").parse((String) keyName);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage());
      }
    }
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.get(Calendar.DAY_OF_WEEK);
    buffer.append(sdf.format(cal.getTime())).append("To");
    cal.add(Calendar.DATE,+6);
    buffer.append(sdf.format(cal.getTime()));
    return buffer.toString();
  }
  
  @SuppressWarnings("unchecked")
  protected Response metricsResponseGenerator(String esResponse, String periodStr,
      Map<String, Object> viewData) {
    Response response = new Response();
    Map<String, Object> responseData = new LinkedHashMap<>();
    try {
      Map<String, Object> esData = mapper.readValue(esResponse, Map.class);
      responseData.putAll(viewData);
      responseData.put(JsonKey.PERIOD, periodStr);
      responseData.put(JsonKey.SNAPSHOT, esData.get(JsonKey.SNAPSHOT));
      responseData.put(JsonKey.SERIES, esData.get(JsonKey.SERIES));
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage());
      // throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage());
      // throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    }
    response.putAll(responseData);
    return response;
  }

}
