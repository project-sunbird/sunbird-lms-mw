package org.sunbird.metrics.actors;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

public class OrganisationMetricsActor extends UntypedAbstractActor {
  
  private static ObjectMapper mapper = new ObjectMapper();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("OrganisationManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CREATION_METRICS.getValue())) {
          orgCreationMetrics(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CONSUMPTION_METRICS.getValue())) {
          orgConsumptionMetrics(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION", LoggerEnum.INFO.name());
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void orgCreationMetrics(Request actorMessage) {
    ProjectLogger.log("In orgCreation metrics");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      /*int days = 0;
      switch(periodStr){
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
      }*/
      //Date endDate = new Date(days); 
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      String query = getQuery(periodStr,orgId);
      String esResponse = getESData(query); 
      String responseFormat = metricsESResponseGenerator(esResponse);
      Response response = metricsResponseGenerator(responseFormat, periodStr, getViewData(orgId));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs" + e.getMessage());
      sender().tell(e, self());
      return;
    }
  }
  
  private Map<String, Object> getViewData(String orgId){
    Map<String,Object> orgData = new HashMap<>();
    Map<String,Object> viewData = new HashMap<>();
    orgData.put(JsonKey.ORG_ID,orgId);
    orgData.put(JsonKey.ORG_NAME, "sunbird");
    viewData.put("org", orgData);
    return viewData;
  }
  private String getQuery(String periodStr, String orgId){
    String query = "{\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"query\":{\"range\":{\"lastUpdatedOn\":{\"gt\":\"2017-07-24T00:00:00.000+0530\",\"lte\":\"2017-08-01T00:00:00.000+0530\"}}}},{\"match\":{\"createdFor.raw\":\"Sunbird\"}}]}}}},\"size\":0,\"aggs\":{\"created_on\":{\"date_histogram\":{\"field\":\"lastUpdatedOn\",\"interval\":\"1d\",\"format\":\"yyyy-MM-dd\"}},\"status\":{\"terms\":{\"field\":\"status.raw\",\"include\":[\"draft\",\"live\",\"review\"]},\"aggs\":{\"updated_on\":{\"date_histogram\":{\"field\":\"lastUpdatedOn\",\"interval\":\"1d\",\"format\":\"yyyy-MM-dd\"}}}},\"authors.count\":{\"cardinality\":{\"field\":\"createdBy.raw\",\"precision_threshold\":100}},\"content_count\":{\"terms\":{\"field\":\"objectType.raw\",\"include\":\"content\"}}}}";
    return query;
  }
  
  private String getESData(String query) {
    Map<String,String> headers = new HashMap<>();
    String response = null;
    try {
      //String baseSearchUrl = System.getenv(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      String baseSearchUrl = "http://localhost:9200/compositesearch/_search";
      if(ProjectUtil.isStringNullOREmpty(baseSearchUrl)){
        baseSearchUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      }
      headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
      if(ProjectUtil.isStringNullOREmpty((String)headers.get(JsonKey.AUTHORIZATION))){
        headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
      }
       /* response = HttpUtil.sendPostRequest(baseSearchUrl+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_SEARCH_URL),
                query, headers);*/
      response = "{\"id\":\"ekstep.analytics.metrics.query-proxy\",\"ver\":\"1.0\",\"ts\":\"2016-09-12T18:43:23.890+00:00\",\"params\":{\"resmsgid\":\"4f04da60-1e24-4d31-aa7b-1daf91c46341\",\"status\":\"successful\"},\"responseCode\":\"OK\",\"result\":\"{\\\"took\\\":844,\\\"timed_out\\\":false,\\\"_shards\\\":{\\\"total\\\":1,\\\"successful\\\":1,\\\"failed\\\":0},\\\"hits\\\":{\\\"total\\\":608,\\\"max_score\\\":0,\\\"hits\\\":[]},\\\"aggregations\\\":{\\\"created_on\\\":{\\\"buckets\\\":[{\\\"key_as_string\\\":\\\"2017-07-24\\\",\\\"key\\\":1500854400000,\\\"doc_count\\\":163},{\\\"key_as_string\\\":\\\"2017-07-25\\\",\\\"key\\\":1500940800000,\\\"doc_count\\\":140},{\\\"key_as_string\\\":\\\"2017-07-26\\\",\\\"key\\\":1501027200000,\\\"doc_count\\\":170},{\\\"key_as_string\\\":\\\"2017-07-27\\\",\\\"key\\\":1501113600000,\\\"doc_count\\\":76},{\\\"key_as_string\\\":\\\"2017-07-28\\\",\\\"key\\\":1501200000000,\\\"doc_count\\\":29},{\\\"key_as_string\\\":\\\"2017-07-29\\\",\\\"key\\\":1501286400000,\\\"doc_count\\\":15},{\\\"key_as_string\\\":\\\"2017-07-30\\\",\\\"key\\\":1501372800000,\\\"doc_count\\\":12},{\\\"key_as_string\\\":\\\"2017-07-31\\\",\\\"key\\\":1501459200000,\\\"doc_count\\\":3}]},\\\"authors.count\\\":{\\\"value\\\":25},\\\"reviewers.count\\\":{\\\"value\\\":25},\\\"content_count\\\":{\\\"doc_count_error_upper_bound\\\":0,\\\"sum_other_doc_count\\\":0,\\\"buckets\\\":[{\\\"key\\\":\\\"content\\\",\\\"doc_count\\\":547}]},\\\"status\\\":{\\\"doc_count_error_upper_bound\\\":0,\\\"sum_other_doc_count\\\":0,\\\"buckets\\\":[{\\\"key\\\":\\\"draft\\\",\\\"doc_count\\\":449,\\\"updated_on\\\":{\\\"buckets\\\":[{\\\"key_as_string\\\":\\\"2017-07-24\\\",\\\"key\\\":1500854400000,\\\"doc_count\\\":112},{\\\"key_as_string\\\":\\\"2017-07-25\\\",\\\"key\\\":1500940800000,\\\"doc_count\\\":98},{\\\"key_as_string\\\":\\\"2017-07-26\\\",\\\"key\\\":1501027200000,\\\"doc_count\\\":121},{\\\"key_as_string\\\":\\\"2017-07-27\\\",\\\"key\\\":1501113600000,\\\"doc_count\\\":72},{\\\"key_as_string\\\":\\\"2017-07-28\\\",\\\"key\\\":1501200000000,\\\"doc_count\\\":23},{\\\"key_as_string\\\":\\\"2017-07-29\\\",\\\"key\\\":1501286400000,\\\"doc_count\\\":15},{\\\"key_as_string\\\":\\\"2017-07-30\\\",\\\"key\\\":1501372800000,\\\"doc_count\\\":8}]}},{\\\"key\\\":\\\"live\\\",\\\"doc_count\\\":88,\\\"updated_on\\\":{\\\"buckets\\\":[{\\\"key_as_string\\\":\\\"2017-07-24\\\",\\\"key\\\":1500854400000,\\\"doc_count\\\":22},{\\\"key_as_string\\\":\\\"2017-07-25\\\",\\\"key\\\":1500940800000,\\\"doc_count\\\":34},{\\\"key_as_string\\\":\\\"2017-07-26\\\",\\\"key\\\":1501027200000,\\\"doc_count\\\":26},{\\\"key_as_string\\\":\\\"2017-07-27\\\",\\\"key\\\":1501113600000,\\\"doc_count\\\":3},{\\\"key_as_string\\\":\\\"2017-07-28\\\",\\\"key\\\":1501200000000,\\\"doc_count\\\":3}]}},{\\\"key\\\":\\\"review\\\",\\\"doc_count\\\":69,\\\"updated_on\\\":{\\\"buckets\\\":[{\\\"key_as_string\\\":\\\"2017-07-24\\\",\\\"key\\\":1500854400000,\\\"doc_count\\\":29},{\\\"key_as_string\\\":\\\"2017-07-25\\\",\\\"key\\\":1500940800000,\\\"doc_count\\\":8},{\\\"key_as_string\\\":\\\"2017-07-26\\\",\\\"key\\\":1501027200000,\\\"doc_count\\\":22},{\\\"key_as_string\\\":\\\"2017-07-27\\\",\\\"key\\\":1501113600000,\\\"doc_count\\\":1},{\\\"key_as_string\\\":\\\"2017-07-28\\\",\\\"key\\\":1501200000000,\\\"doc_count\\\":3},{\\\"key_as_string\\\":\\\"2017-07-29\\\",\\\"key\\\":1501286400000,\\\"doc_count\\\":0},{\\\"key_as_string\\\":\\\"2017-07-30\\\",\\\"key\\\":1501372800000,\\\"doc_count\\\":4},{\\\"key_as_string\\\":\\\"2017-07-31\\\",\\\"key\\\":1501459200000,\\\"doc_count\\\":2}]}}]}}}\"}";
        
    } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
    }
    return response;
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  private String metricsESResponseGenerator(String responseStr){
    String result = null;
    Map<String, Object> responseMap = new HashMap<>();
    try {
      Map<String,Object> esData = mapper.readValue(responseStr, Map.class);
      String resultStr = (String) esData.get(JsonKey.RESULT);
      Map<String,Object> resultData = mapper.readValue(resultStr, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.AGGREGATIONS);
      
      Map<String,Object> snapshot = new LinkedHashMap<>();
      Map<String,Object> dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of contents created" );
      dataMap.put(JsonKey.VALUE,340);
      snapshot.put("org.creation.content.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of authors" );
      dataMap.putAll((Map<String,Object>)resultData.get("authors.count"));
      snapshot.put("org.creation.authors.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of reviewers" );
      dataMap.putAll((Map<String,Object>)resultData.get("reviewers.count"));
      snapshot.put("org.creation.reviewers.count", dataMap);
      dataMap = new HashMap<>();
      Object value = null;
      Map<String,Object> valueMap = new HashMap<>(); 
      valueMap = (Map<String,Object>) resultData.get("status");
      List<Map<String,Object>> valueList = (List<Map<String,Object>>) valueMap.get("buckets");
      valueMap = new HashMap<>();
      for(Map<String,Object> data : valueList){
        if("live".equalsIgnoreCase((String)data.get("key"))){
          valueMap.put("live",data.get("doc_count"));
        }else if ("draft".equalsIgnoreCase((String)data.get("key"))){
          valueMap.put("draft", data.get("doc_count"));
        }else if("review".equalsIgnoreCase((String)data.get("key"))){
          valueMap.put("review", data.get("doc_count"));
        }
      }
      value = valueMap.get("live");
      dataMap.put(JsonKey.NAME, "Number of content items published");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=published].count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content items created");
      value = valueMap.get("draft");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=draft].count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content items reviewed");
      value = valueMap.get("review");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=review].count", dataMap);
      
      
      Map<String, Object> series = new HashMap<>();
      Map aggKeyMap = (Map) resultData.get("created_on");
      List<Map<String, Object>> bucket = getBucketData(aggKeyMap);
      Map<String,Object> seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Content created by day");
      seriesData.put(JsonKey.SPLIT, "content.created_on");
      seriesData.put(JsonKey.TIME_UNIT, "seconds");
      seriesData.put("buckets", bucket);
      series.put("org.creation.content.created_on.count", seriesData);
      
      seriesData = new LinkedHashMap<>();
      bucket = getBucketData(aggKeyMap);
      
      aggKeyMap = (Map) resultData.get("status");
      bucket = (List<Map<String, Object>>) aggKeyMap.get("buckets");
      Map<String, Object> statusList = new HashMap();
      List<Map<String, Object>> statusBucket = new ArrayList<>();
      for(Map<String,Object> status: bucket){
        seriesData = new LinkedHashMap<>();
        if("live".equalsIgnoreCase((String)status.get("key"))){
          statusList = (Map<String, Object>) status.get("updated_on");
          statusBucket = getBucketData(statusList);
          seriesData.put(JsonKey.NAME, "Live");
          seriesData.put(JsonKey.SPLIT, "content.published_on");
          seriesData.put(JsonKey.TIME_UNIT, "seconds");
          seriesData.put("buckets", statusBucket);
          series.put("org.creation.content[@status=published].published_on.count", seriesData);
        }else if("draft".equalsIgnoreCase((String)status.get("key"))){
          statusList = (Map<String, Object>) status.get("updated_on");
          statusBucket = getBucketData(statusList);
          seriesData.put(JsonKey.NAME, "Draft");
          seriesData.put(JsonKey.SPLIT, "content.created_on");
          seriesData.put(JsonKey.TIME_UNIT, "seconds");
          seriesData.put("buckets", statusBucket);
          series.put("org.creation.content[@status=draft].count", seriesData);
        }else if("review".equalsIgnoreCase((String)status.get("key"))){
          statusList = (Map<String, Object>) status.get("updated_on");
          statusBucket = getBucketData(statusList);
          seriesData.put(JsonKey.NAME, "Review");
          seriesData.put(JsonKey.SPLIT, "content.reviewed_on");
          seriesData.put(JsonKey.TIME_UNIT, "seconds");
          seriesData.put("buckets", statusBucket);
          series.put("org.creation.content[@status=review].count", seriesData);
        }
      }
      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);
      
      result = mapper.writeValueAsString(responseMap);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage());
      //throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage());
      //throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    }
    return result;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private List<Map<String, Object>> getBucketData(Map aggKeyMap) {
    List<Map<String, Double>> aggKeyList = (List<Map<String, Double>>) aggKeyMap.get("buckets");
    List<Map<String, Object>> parentGroupList = new ArrayList<Map<String, Object>>();
    for (Map aggKeyListMap : aggKeyList) {
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        parentCountObject.put("key", aggKeyListMap.get("key"));
        parentCountObject.put("key_name", aggKeyListMap.get("key_as_string"));
        parentCountObject.put("value", aggKeyListMap.get("doc_count"));
        parentGroupList.add(parentCountObject);
    }
    return parentGroupList;
  }
  
  @SuppressWarnings("unchecked")
  private Response metricsResponseGenerator(String esResponse, String periodStr, Map<String,Object> viewData){
    Response response = new Response();
    Map<String,Object> responseData =  new LinkedHashMap<>();
    try {
    Map<String,Object> esData = mapper.readValue(esResponse, Map.class);
    responseData.putAll(viewData);
    responseData.put(JsonKey.PERIOD, periodStr);
    responseData.put(JsonKey.SNAPSHOT, esData.get(JsonKey.SNAPSHOT));
    responseData.put(JsonKey.SERIES, esData.get(JsonKey.SERIES));
    }catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage());
      //throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage());
      //throw new ProjectCommonException("", "", ResponseCode.SERVER_ERROR.getResponseCode());
    }
    response.putAll(responseData);
    return response;
  }
  
  @SuppressWarnings("unchecked")
  private void orgConsumptionMetrics(Request actorMessage) {
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    Map<String, Object> filter = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    filter.put(JsonKey.TAG, orgId);
    //requestMap.put(JsonKey.CHANNEL, getChannel());
    request.setRequest(requestMap);
    String requestStr;
    Map<String,Object> resultData = new HashMap<>();
    try {
      requestStr = mapper.writeValueAsString(request);
      String resp = getDataFromEkstep(requestStr);
      Map<String,Object> ekstepResponse = mapper.readValue(resp, Map.class);
      resultData = (Map<String,Object>) ekstepResponse.get(JsonKey.RESULT);
    } catch (JsonProcessingException e) {
     ProjectLogger.log(e.getMessage(),e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);
    }
    Map<String, Object> responseMap = new LinkedHashMap<>();
    Map<String,Object> snapshot = new LinkedHashMap<>();
    Map<String,Object> dataMap = new HashMap<>();
    dataMap.put(JsonKey.NAME, "Number of visits by users" );
    dataMap.put(JsonKey.VALUE,"345");
    snapshot.put("org.consumption.content.session.count", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Content consumption time" );
    dataMap.put(JsonKey.VALUE,"23400");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    snapshot.put("org.consumption.content.time_spent.sum", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Average time spent by user per visit" );
    dataMap.put(JsonKey.VALUE,"512");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    snapshot.put("org.consumption.content.time_spent.average", dataMap);
    dataMap = new LinkedHashMap<>();
    List<Map<String,Object>> valueMap = new ArrayList<>();
    List<Map<String, Object>> bucket = new ArrayList<>();
    valueMap = (List<Map<String,Object>>) resultData.get("metrics");
    try {
      for(int count=0;count<7;count++){
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        String value = "2017-07-2"+(count);
        parentCountObject.put("key", new SimpleDateFormat("yyyy-MM-dd").parse(value).getTime());
        parentCountObject.put("key_name", value);
        parentCountObject.put("value", count*8);
        bucket.add(parentCountObject);
      }
    }catch(Exception e){
      ProjectLogger.log(e.getMessage(), e);
    }
    Map<String, Object> series = new HashMap<>();
    
    Map<String,Object> seriesData = new LinkedHashMap<>();
    seriesData.put(JsonKey.NAME, "Time spent by day");
    seriesData.put(JsonKey.SPLIT, "content.session.start_time");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    seriesData.put("buckets", bucket);
    series.put("org.consumption.content.time_spent.sum", seriesData);
    responseMap.putAll(getViewData(orgId));
    responseMap.put(JsonKey.PERIOD, periodStr);
    responseMap.put(JsonKey.SNAPSHOT, snapshot);
    responseMap.put(JsonKey.SERIES, series);
    Response response = new Response();
    response.putAll(responseMap);
    sender().tell(response, self()); 
  }
  
  private String getDataFromEkstep(String request){
    Map<String,String> headers = new HashMap<>();
    String response = null;
    try {
      String baseSearchUrl = System.getenv(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      if(ProjectUtil.isStringNullOREmpty(baseSearchUrl)){
        baseSearchUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
      }
      //TODO:Remove this once mockup api's are replaced
      baseSearchUrl = "https://dev.ekstep.in/api";
      headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
      if(ProjectUtil.isStringNullOREmpty((String)headers.get(JsonKey.AUTHORIZATION))){
        headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
      }
      response = HttpUtil.sendPostRequest(baseSearchUrl+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_METRICS_URL),
                request, headers);
      
    } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
    }
    return response;
  }

}
