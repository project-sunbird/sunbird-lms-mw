package org.sunbird.metrics.actors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;

import akka.actor.UntypedAbstractActor;

public abstract class BaseMetricsActor extends UntypedAbstractActor {

  protected abstract Map<String, Object> getViewData(String id, Object data);

  protected Map<String, Object> addSnapshot(String keyName, String name, Object value,
      String timeUnit) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put(JsonKey.NAME, name);
    snapshot.put(JsonKey.VALUE, value);
    if (!ProjectUtil.isStringNullOREmpty(timeUnit)) {
      snapshot.put(JsonKey.TIME_UNIT, timeUnit);
    }
    return snapshot;
  }

  protected static Map<String, String> getStartAndEndDate(String period) {
    Map<String, String> dateMap = new HashMap<>();
    int days = getDaysByPeriod(period);
    Date endDate = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(endDate.getTime());
    cal.add(Calendar.DATE, -days);
    String startDateStr = sdf.format(cal.getTimeInMillis());
    String endDateStr = sdf.format(endDate.getTime());
    dateMap.put("startDate", startDateStr);
    dateMap.put("endDate", endDateStr);
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
        days = 36;
        break;
      }
    }
    return days;
  }
  
  protected List<Map<String,Object>> createBucketStructure(String periodStr) {
    int days = getDaysByPeriod(periodStr);
    Date date = new Date();
    List<Map<String,Object>> bucket = new ArrayList<>();
    for(int day = 0; day < days; day++){
      Map<String, Object> bucketData = new LinkedHashMap<String, Object>();
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(date.getTime());
      cal.add(Calendar.DATE, -days);
      bucketData.put("key", cal.getTimeInMillis());
      bucketData.put("key_name", new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime()));
      bucketData.put("value", 0);
      bucket.add(bucketData);
    }
    return bucket;
  }

  protected String getDataFromEkstep(String request, String apiUrl) {
    Map<String, String> headers = new HashMap<>();
    String response = null;
    try {
      String baseSearchUrl = System.getenv(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      if (ProjectUtil.isStringNullOREmpty(baseSearchUrl)) {
        baseSearchUrl =
            PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      }
      // TODO:Remove this once mockup api's are replaced
      baseSearchUrl = "https://dev.ekstep.in/api";
      headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
      if (ProjectUtil.isStringNullOREmpty((String) headers.get(JsonKey.AUTHORIZATION))) {
        headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER
            + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
        headers.put("Content_Type", "application/json");
      }
      response = HttpUtil.sendPostRequest(
          baseSearchUrl + PropertiesCache.getInstance().getProperty(apiUrl), request, headers);

    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return response;
  }

  public static String makePostRequest(String url, String body) throws Exception {
    String baseSearchUrl = "https://dev.ekstep.in/api";
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(baseSearchUrl + PropertiesCache.getInstance().getProperty(url));
    post.addHeader("Content-Type", "application/json; charset=utf-8");
    post.setEntity(new StringEntity(body, Charsets.UTF_8.name()));
    HttpResponse response = client.execute(post);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new Exception("Service unavailable: " + response.getStatusLine().getStatusCode() + " : "
          + response.getStatusLine().getReasonPhrase());
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

}
