package org.sunbird.metrics.actors;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrganisationMetricsActor extends BaseMetricsActor {

  private static ObjectMapper mapper = new ObjectMapper();
  private static final String view = "org";
  private static List<String> operationList = new ArrayList<>();

  static {
    operationList.add("Create");
    operationList.add("Review");
    operationList.add("Publish");
  }

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
          orgConsumptionMetricsMock(actorMessage);
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

  @Override
  protected Map<String, Object> getViewData(String orgId, Object orgName) {
    Map<String, Object> orgData = new HashMap<>();
    Map<String, Object> viewData = new HashMap<>();
    orgData.put(JsonKey.ORG_ID, orgId);
    orgData.put(JsonKey.ORG_NAME, orgName);
    viewData.put("org", orgData);
    return viewData;
  }

  private String getQueryRequest(String periodStr, String orgId, String operation) {
    ProjectLogger.log("orgId " + orgId);
    Map<String, Object> dateMap = getStartAndEndDate(periodStr);
    Map<String, String> operationMap = new LinkedHashMap<>();
    ProjectLogger.log("period" + dateMap);
    switch (operation) {
      case "Create": {
        operationMap.put("dateKey", "createdOn");
        operationMap.put("status", "Draft");
        operationMap.put("userActionKey", "createdBy");
        operationMap.put("contentCount", "required");
        break;
      }
      case "Review": {
        operationMap.put("dateKey", "lastSubmittedOn");
        operationMap.put("status", "Review");
        break;
      }
      case "Publish": {
        operationMap.put("dateKey", "lastPublishedOn");
        operationMap.put("status", "Live");
        operationMap.put("userActionKey", "lastPublishedBy");
        break;
      }
    }
    StringBuilder builder = new StringBuilder();
    builder.append("{\"request\":{\"rawQuery\":{\"query\":{\"filtered\":")
        .append("{\"query\":{\"bool\":{\"must\":[{\"query\":{\"range\":{\"")
        .append(operationMap.get("dateKey")).append("\":{\"gte\":\"")
        .append(dateMap.get("startDate") + "\",\"lte\":\"" + dateMap.get("endDate") + "\"}}}}")
        .append(",{\"bool\":{\"should\":[{\"match\":{\"contentType.raw\":\"Story\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"Worksheet\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"Game\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"Collection\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"TextBook\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"TextBookUnit\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"Course\"}}")
        .append(",{\"match\":{\"contentType.raw\":\"CourseUnit\"}}]}},")
        .append("{\"match\":{\"createdFor.raw\":\"" + orgId + "\"}}")
        .append(",{\"match\":{\"status.raw\":\"" + operationMap.get("status"))
        .append("\"}}]}}}},\"aggs\":{\"");
    if (operationMap.containsValue("createdOn")) {
      builder.append(operationMap.get("dateKey") + "\":{\"date_histogram\":{\"field\":\"")
          .append(operationMap.get("dateKey"))
          .append("\",\"interval\":\"1d\",\"format\":\"yyyy-MM-dd\"")
          .append(",\"time_zone\":\"+05:30\",\"extended_bounds\":{\"min\":")
          .append(dateMap.get("startTimeMilis") + ",\"max\":")
          .append(dateMap.get("endTimeMilis") + "}}},\"");
    }
    builder.append("status\":{\"terms\":{\"field\":\"status.raw\",\"include\":[\"")
        .append(operationMap.get("status").toLowerCase() + "\"]},\"aggs\":{\"")
        .append(operationMap.get("dateKey") + "\":{\"date_histogram\":{\"field\":\"")
        .append(operationMap.get("dateKey") + "\",\"interval\":\"1d\",\"format\":")
        .append("\"yyyy-MM-dd\",\"time_zone\":\"+05:30\",\"extended_bounds\":{\"min\":")
        .append(dateMap.get("startTimeMilis") + ",\"max\":").append(dateMap.get("endTimeMilis"))
        .append("}}}}}");
    if (operationMap.containsKey("userActionKey")) {
      builder.append(",\"" + operationMap.get("userActionKey") + ".count\":")
          .append("{\"cardinality\":{\"field\":\"" + operationMap.get("userActionKey"))
          .append(".raw\",\"precision_threshold\":100}}");
    }

    if (operationMap.containsKey("contentCount")) {
      builder.append(",\"content_count\":{\"terms\":{\"field\":\"contentType.raw\"")
          .append(",\"exclude\":[\"assets\",\"plugin\",\"template\"]}},")
          .append("\"total_content_count\":{\"sum_bucket\":")
          .append("{\"buckets_path\":\"content_count>_count\"}}");
    }
    builder.append("}}}}");
    return builder.toString();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private List<Map<String, Object>> getBucketData(Map aggKeyMap) {
    List<Map<String, Object>> parentGroupList = new ArrayList<Map<String, Object>>();
    if(null==aggKeyMap || aggKeyMap.isEmpty()){
      return parentGroupList;
    }
    List<Map<String, Double>> aggKeyList = (List<Map<String, Double>>) aggKeyMap.get("buckets");
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
  private Response metricsResponseGenerator(String esResponse, String periodStr,
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

  /*private static Map<String, Object> getAggregationMap() {
    return aggregationMap;
  }*/

  @SuppressWarnings("unchecked")
  private static Map<String, Object> putAggregationMap(String responseStr, Map<String, Object> aggregationMap, String operation){
    try {
      Map<String, Object> resultData = mapper.readValue(responseStr, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      resultData = (Map<String, Object>) resultData.get(JsonKey.AGGREGATIONS);
      List<Map<String,Object>> statusList = new ArrayList<>();
      for(Map.Entry<String, Object> data: resultData.entrySet()){
        if("status".equalsIgnoreCase(data.getKey())){
          Map<String,Object> statusMap = new HashMap<>();
          statusMap.put(operation, data.getValue());
          statusList = (List<Map<String, Object>>) aggregationMap.get(data.getKey());
          if(null==statusList){
            statusList = new ArrayList<>();
          }
          statusList.add(statusMap);
          aggregationMap.put(data.getKey(), statusList);
        }else {
          aggregationMap.put(data.getKey(),data.getValue());
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return aggregationMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private String metricsOrgResponseGenerator(String periodStr, Map<String, Object> resultData) {
    String dataSet = "creation";
    String result = null;
    Map<String, Object> responseMap = new HashMap<>();
    try {
      Map<String, Object> snapshot = new LinkedHashMap<>();
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content created");
      Map<String, Object> contentData = (Map<String, Object>) resultData.get("total_content_count");
      if (null != contentData && !contentData.isEmpty()) {
        dataMap.put(JsonKey.VALUE, contentData.get(JsonKey.VALUE));
      } else {
        dataMap.put(JsonKey.VALUE, 0);
      }
      snapshot.put("org.creation.content.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of authors");
      if(null!=resultData.get("createdBy.count")){
        dataMap.putAll((Map<String, Object>) resultData.get("createdBy.count"));
      } else {
        dataMap.put(JsonKey.VALUE, 0);
      }
      snapshot.put("org.creation.authors.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of reviewers");
      if(null!=resultData.get("lastPublishedBy.count")){
        dataMap.putAll((Map<String, Object>) resultData.get("lastPublishedBy.count"));
      } else {
        dataMap.put(JsonKey.VALUE, 0);
      }
      snapshot.put("org.creation.reviewers.count", dataMap);
      dataMap = new HashMap<>();
      Object value = null;
      List<Map<String,Object>> valueMapList = (List<Map<String, Object>>) resultData.get("status");
      Map<String, Object> statusValueMap = new HashMap<>();
      statusValueMap.put("live", 0);
      statusValueMap.put("draft", 0);
      statusValueMap.put("review", 0);
      for(Map<String, Object> data: valueMapList){
        Map<String, Object> statusMap = new HashMap<>();
        List<Map<String, Object>> valueList = new ArrayList<>();
        if(data.containsKey("Create")){
          statusMap = (Map<String, Object>) data.get("Create");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if(!valueList.isEmpty()){
            statusMap = valueList.get(0);
            statusValueMap.put("draft",statusMap.get("doc_count"));
            statusValueMap.put("draftBucket", statusMap.get("createdOn"));
          }
        }else if(data.containsKey("Publish")){
          statusMap = (Map<String, Object>) data.get("Publish");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if(!valueList.isEmpty()){
            statusMap = valueList.get(0);
            statusValueMap.put("live",statusMap.get("doc_count"));
            statusValueMap.put("liveBucket",statusMap.get("lastPublishedOn"));
          }
        }else if(data.containsKey("Review")){
          statusMap = (Map<String, Object>) data.get("Review");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if(!valueList.isEmpty()){
            statusMap = valueList.get(0);
            statusValueMap.put("review",statusMap.get("doc_count"));
            statusValueMap.put("reviewBucket", statusMap.get("lastSubmittedOn"));
          }
        }
      }
      
      value = statusValueMap.get("live");
      dataMap.put(JsonKey.NAME, "Number of content items published");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=published].count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content items created");
      value = statusValueMap.get("draft");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=draft].count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content items reviewed");
      value = statusValueMap.get("review");
      dataMap.put(JsonKey.VALUE, value);
      snapshot.put("org.creation.content[@status=review].count", dataMap);

      Map<String, Object> series = new HashMap<>();
      Map aggKeyMap = (Map) resultData.get("createdOn");
      List<Map<String, Object>> bucket = getBucketData(aggKeyMap);
      Map<String, Object> seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Content created by day");
      seriesData.put(JsonKey.SPLIT, "content.created_on");
      if(null==bucket || bucket.isEmpty()){
        bucket = createBucketStructure(periodStr);
      }
      seriesData.put("buckets", bucket);
      series.put("org.creation.content.created_on.count", seriesData);
      
      Map<String, Object> statusList = new HashMap();
      List<Map<String, Object>> statusBucket = new ArrayList<>();
      statusList = (Map<String, Object>) statusValueMap.get("draftBucket");
      statusBucket = getBucketData(statusList);
      if(null==statusBucket||statusBucket.isEmpty()){
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Draft");
      seriesData.put(JsonKey.SPLIT, "content.created_on");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=draft].count", seriesData);
      
      statusList = (Map<String, Object>) statusValueMap.get("liveBucket");
      statusBucket = getBucketData(statusList);
      if(null==statusBucket||statusBucket.isEmpty()){
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Live");
      seriesData.put(JsonKey.SPLIT, "content.published_on");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=published].published_on.count", seriesData);
      
      statusList = (Map<String, Object>) statusValueMap.get("reviewBucket");
      statusBucket = getBucketData(statusList);
      if(null==statusBucket||statusBucket.isEmpty()){
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Review");
      seriesData.put(JsonKey.SPLIT, "content.reviewed_on");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=review].count", seriesData);
      
      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);

      result = mapper.writeValueAsString(responseMap);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage());
    }
    return result;
  }



  @SuppressWarnings("unchecked")
  private void orgConsumptionMetricsMock(Request actorMessage) {
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    Map<String, Object> filter = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    filter.put(JsonKey.TAG, orgId);
    // requestMap.put(JsonKey.CHANNEL, getChannel());
    request.setRequest(requestMap);
    String requestStr;
    Map<String, Object> resultData = new HashMap<>();
    try {
      requestStr = mapper.writeValueAsString(request);
      String resp = getDataFromEkstep(requestStr, JsonKey.EKSTEP_METRICS_URL);
      Map<String, Object> ekstepResponse = mapper.readValue(resp, Map.class);
      resultData = (Map<String, Object>) ekstepResponse.get(JsonKey.RESULT);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    Map<String, Object> responseMap = new LinkedHashMap<>();
    Map<String, Object> snapshot = new LinkedHashMap<>();
    Map<String, Object> dataMap = new HashMap<>();
    dataMap.put(JsonKey.NAME, "Number of visits by users");
    dataMap.put(JsonKey.VALUE, "345");
    snapshot.put("org.consumption.content.session.count", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Content consumption time");
    dataMap.put(JsonKey.VALUE, "23400");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    snapshot.put("org.consumption.content.time_spent.sum", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Average time spent by user per visit");
    dataMap.put(JsonKey.VALUE, "512");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    snapshot.put("org.consumption.content.time_spent.average", dataMap);
    dataMap = new LinkedHashMap<>();
    List<Map<String, Object>> valueMap = new ArrayList<>();
    List<Map<String, Object>> bucket = new ArrayList<>();
    valueMap = (List<Map<String, Object>>) resultData.get("metrics");
    try {
      for (int count = 0; count < 7; count++) {
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        String value = "2017-07-2" + (count);
        parentCountObject.put("key", new SimpleDateFormat("yyyy-MM-dd").parse(value).getTime());
        parentCountObject.put("key_name", value);
        parentCountObject.put("value", count * 8);
        bucket.add(parentCountObject);
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    Map<String, Object> series = new HashMap<>();

    Map<String, Object> seriesData = new LinkedHashMap<>();
    seriesData.put(JsonKey.NAME, "Time spent by day");
    seriesData.put(JsonKey.SPLIT, "content.session.start_time");
    dataMap.put(JsonKey.TIME_UNIT, "seconds");
    seriesData.put("buckets", bucket);
    series.put("org.consumption.content.time_spent.sum", seriesData);
    responseMap.putAll(getViewData(orgId, null));
    responseMap.put(JsonKey.PERIOD, periodStr);
    responseMap.put(JsonKey.SNAPSHOT, snapshot);
    responseMap.put(JsonKey.SERIES, series);
    Response response = new Response();
    response.putAll(responseMap);
    sender().tell(response, self());
  }

  private void orgCreationMetrics(Request actorMessage) {
    ProjectLogger.log("In orgCreationMetrics api");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      String orgName = validateOrg(orgId);
      if (ProjectUtil.isStringNullOREmpty(orgName)) {
       /* ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());*/
        // return;
      }
      Map<String, Object> aggregationMap = new HashMap<>();
      for (String operation : operationList) {
        String request = getQueryRequest(periodStr, orgId, operation);
        String esResponse = makePostRequest(JsonKey.EKSTEP_ES_METRICS_URL, request);
        //String esResponse = getDataFromEkstep(request, JsonKey.EKSTEP_ES_METRICS_URL);
        aggregationMap = putAggregationMap(esResponse, aggregationMap, operation);
      }
      String responseFormat = metricsOrgResponseGenerator(periodStr, aggregationMap);
      Response response =
          metricsResponseGenerator(responseFormat, periodStr, getViewData(orgId, orgName));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs",e);
      sender().tell(e, self());
      return;
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs",e);
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private void orgConsumptionMetrics(Request actorMessages) {

  }

  private String validateOrg(String orgId) {
    String orgName = null;
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(), orgId);
    if (null == result || result.isEmpty()) {
      return null;
    }
    orgName = (String) result.get(JsonKey.ORG_NAME);
    return orgName;
  }

}
