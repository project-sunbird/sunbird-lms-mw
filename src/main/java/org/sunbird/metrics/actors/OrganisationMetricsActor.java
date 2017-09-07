package org.sunbird.metrics.actors;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.ActorRef;
import akka.actor.Props;

public class OrganisationMetricsActor extends BaseMetricsActor {

  private static ObjectMapper mapper = new ObjectMapper();
  private static final String view = "org";
  private static List<String> operationList = new ArrayList<>();
  private static Map<String, String> conceptsList = new HashMap<>();
  private ActorRef backGroundActorRef;
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
  
  public OrganisationMetricsActor() {
    backGroundActorRef = getContext().actorOf(Props.create(MetricsBackGroundJobActor.class), "metricsBackGroundJobActor");
  }

  protected enum ContentStatus {
    Draft("Create"), Review("Review"), Live("Publish");

    private String contentOperation;

    private ContentStatus(String operation) {
      this.contentOperation = operation;
    }

    private String getOperation() {
      return this.contentOperation;
    }

  }

  static {
    operationList.add("Create");
    operationList.add("Review");
    operationList.add("Publish");
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("OrganisationMetricsActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CREATION_METRICS.getValue())) {
          orgCreationMetrics(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CONSUMPTION_METRICS.getValue())) {
          orgConsumptionMetrics(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CREATION_METRICS_REPORT.getValue())) {
          orgCreationMetricsReport(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CONSUMPTION_METRICS_REPORT.getValue())) {
          orgConsumptionMetricsReport(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CREATION_METRICS_DATA.getValue())) {
          orgCreationMetricsData(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ORG_CONSUMPTION_METRICS_DATA.getValue())) {
          orgConsumptionMetricsData(actorMessage);
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

  private void orgConsumptionMetricsReport(Request actorMessage) {
    ProjectLogger.log("OrganisationMetricsActor-orgConsumptionMetricsReport called");
    String requestId = createReportTrackingEntry(actorMessage);

    Response response = new Response();
    response.put(JsonKey.REQUEST_ID , requestId);
    sender().tell(response, self());
 
 // assign the back ground task to background job actor ...
    Request backGroundRequest = new Request();
    backGroundRequest.setOperation(ActorOperations.PROCESS_DATA.getValue());

    Map<String , Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUEST_ID , requestId);
    innerMap.put(JsonKey.REQUEST , JsonKey.OrgConsumption);
    
    backGroundRequest.setRequest(innerMap);
    backGroundActorRef.tell(backGroundRequest , self());
    return;
  }

  private String createReportTrackingEntry(Request actorMessage) {
    String requestedBy = (String) actorMessage.get(JsonKey.REQUESTED_BY);
    String orgId = (String) actorMessage.get(JsonKey.ORG_ID);
    String period = (String) actorMessage.get(JsonKey.PERIOD);

    Map<String , Object> requestedByInfo = ElasticSearchUtil.getDataByIdentifier(EsIndex.sunbird.getIndexName() , EsType.user.getTypeName() ,requestedBy);
    if(ProjectUtil.isNull(requestedByInfo) || ProjectUtil.isStringNullOREmpty((String)requestedByInfo.get(JsonKey.FIRST_NAME))){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    Map<String, Object> orgData = validateOrg(orgId);
    if (null == orgData) {
      throw new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
          ResponseCode.invalidOrgData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    String requestId = ProjectUtil.getUniqueIdFromTimestamp(1);

    Map<String , Object> requestDbInfo = new HashMap<>();
    requestDbInfo.put(JsonKey.ID , requestId);
    requestDbInfo.put(JsonKey.USER_ID, requestedBy);
    requestDbInfo.put(JsonKey.FIRST_NAME, requestedByInfo.get(JsonKey.FIRST_NAME));
    requestDbInfo.put(JsonKey.STATUS, ReportTrackingStatus.NEW.getValue());
    requestDbInfo.put(JsonKey.RESOURCE_ID , orgId);
    requestDbInfo.put(JsonKey.PERIOD , period);
    requestDbInfo.put(JsonKey.CREATED_DATE , format.format(new Date()));
    requestDbInfo.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    requestDbInfo.put(JsonKey.EMAIL, requestedByInfo.get(JsonKey.EMAIL));
   
    cassandraOperation.insertRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        requestDbInfo);
    
    return requestId;
  }
  
  private void saveData(List<List<Object>> data, String requestId){
    Map<String , Object> dbReqMap = new HashMap<>();
    dbReqMap.put(JsonKey.ID , requestId);
    dbReqMap.put(JsonKey.DATA, data);
    dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.GENERATING_DATA.getValue());
    dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    
    cassandraOperation
    .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        dbReqMap);
  }

  private String getJsonString(Object requestObject) {
    ObjectMapper mapper = new ObjectMapper();
    String data = "";
    try {
      data = mapper.writeValueAsString(requestObject);
    } catch (JsonProcessingException e) {
      throw new ProjectCommonException(ResponseCode.invalidJsonData.getErrorCode(),
          ResponseCode.invalidJsonData.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    return data;
  }

  private void orgCreationMetricsReport(Request actorMessage) {
    ProjectLogger.log("OrganisationMetricsActor-orgCreationMetricsReport called");
    String requestId = createReportTrackingEntry(actorMessage);
    Response response = new Response();
    response.put(JsonKey.REQUEST_ID , requestId);
    sender().tell(response, self());
    
 // assign the back ground task to background job actor ...
    Request backGroundRequest = new Request();
    backGroundRequest.setOperation(ActorOperations.PROCESS_DATA.getValue());

    Map<String , Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUEST_ID , requestId);
    innerMap.put(JsonKey.REQUEST , JsonKey.OrgCreation);
    backGroundRequest.setRequest(innerMap);
    backGroundActorRef.tell(backGroundRequest , self());
    return;
  }

  @Override
  protected Map<String, Object> getViewData(String orgId, Object orgName) {
    Map<String, Object> orgData = new HashMap<>();
    Map<String, Object> viewData = new HashMap<>();
    orgData.put(JsonKey.ORG_ID, orgId);
    orgData.put(JsonKey.ORG_NAME, orgName);
    viewData.put(view, orgData);
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
        operationMap.put("status", ContentStatus.Draft.name());
        operationMap.put("userActionKey", "createdBy");
        operationMap.put("contentCount", "required");
        break;
      }
      case "Review": {
        operationMap.put("dateKey", "lastSubmittedOn");
        operationMap.put("status", ContentStatus.Review.name());
        break;
      }
      case "Publish": {
        operationMap.put("dateKey", "lastPublishedOn");
        operationMap.put("status", ContentStatus.Live.name());
        operationMap.put("userActionKey", "lastPublishedBy");
        break;
      }
    }
    StringBuilder builder = new StringBuilder();
    builder.append("{\"request\":{\"rawQuery\":{\"query\":{\"filtered\":")
        .append("{\"query\":{\"bool\":{\"must\":[{\"query\":{\"range\":{\"")
        .append(operationMap.get("dateKey")).append("\":{\"gte\":\"")
        .append(dateMap.get(startDate) + "\",\"lte\":\"" + dateMap.get(endDate) + "\"}}}}")
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
          .append("\",\"interval\":\"" + dateMap.get(INTERVAL) + "\",\"format\":\"")
          .append(dateMap.get(FORMAT) + "\"")
          .append(",\"time_zone\":\"+05:30\",\"extended_bounds\":{\"min\":")
          .append(dateMap.get(startTimeMilis) + ",\"max\":")
          .append(dateMap.get(endTimeMilis) + "}}},\"");
    }
    builder.append("status\":{\"terms\":{\"field\":\"status.raw\",\"include\":[\"")
        .append(operationMap.get("status").toLowerCase() + "\"]},\"aggs\":{\"")
        .append(operationMap.get("dateKey") + "\":{\"date_histogram\":{\"field\":\"")
        .append(operationMap.get("dateKey") + "\",\"interval\":\"" + dateMap.get(INTERVAL))
        .append("\",\"format\":\"" + dateMap.get(FORMAT))
        .append("\",\"time_zone\":\"+05:30\",\"extended_bounds\":{\"min\":")
        .append(dateMap.get(startTimeMilis) + ",\"max\":").append(dateMap.get(endTimeMilis))
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

  @SuppressWarnings("unchecked")
  private static Map<String, Object> putAggregationMap(String responseStr,
      Map<String, Object> aggregationMap, String operation) {
    try {
      Map<String, Object> resultData = mapper.readValue(responseStr, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      resultData = (Map<String, Object>) resultData.get(JsonKey.AGGREGATIONS);
      List<Map<String, Object>> statusList = new ArrayList<>();
      for (Map.Entry<String, Object> data : resultData.entrySet()) {
        if ("status".equalsIgnoreCase(data.getKey())) {
          Map<String, Object> statusMap = new HashMap<>();
          statusMap.put(operation, data.getValue());
          statusList = (List<Map<String, Object>>) aggregationMap.get(data.getKey());
          if (null == statusList) {
            statusList = new ArrayList<>();
          }
          statusList.add(statusMap);
          aggregationMap.put(data.getKey(), statusList);
        } else {
          aggregationMap.put(data.getKey(), data.getValue());
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return aggregationMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private String orgCreationResponseGenerator(String periodStr, Map<String, Object> resultData) {
    // String dataSet = "creation";
    String result = null;
    Map<String, Object> responseMap = new HashMap<>();
    try {
      Map<String, Object> snapshot = new LinkedHashMap<>();
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of contents created");
      Map<String, Object> contentData = (Map<String, Object>) resultData.get("total_content_count");
      if (null != contentData && !contentData.isEmpty()) {
        dataMap.put(VALUE, contentData.get(VALUE));
      } else {
        dataMap.put(VALUE, 0);
      }
      snapshot.put("org.creation.content.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of authors");
      if (null != resultData.get("createdBy.count")) {
        dataMap.putAll((Map<String, Object>) resultData.get("createdBy.count"));
      } else {
        dataMap.put(VALUE, 0);
      }
      snapshot.put("org.creation.authors.count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of reviewers");
      if (null != resultData.get("lastPublishedBy.count")) {
        dataMap.putAll((Map<String, Object>) resultData.get("lastPublishedBy.count"));
      } else {
        dataMap.put(VALUE, 0);
      }
      snapshot.put("org.creation.reviewers.count", dataMap);
      dataMap = new HashMap<>();
      Object value = null;
      List<Map<String, Object>> valueMapList = (List<Map<String, Object>>) resultData.get("status");
      Map<String, Object> statusValueMap = new HashMap<>();
      statusValueMap.put("live", 0);
      statusValueMap.put("draft", 0);
      statusValueMap.put("review", 0);
      for (Map<String, Object> data : valueMapList) {
        Map<String, Object> statusMap = new HashMap<>();
        List<Map<String, Object>> valueList = new ArrayList<>();
        if (data.containsKey("Create")) {
          statusMap = (Map<String, Object>) data.get("Create");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if (!valueList.isEmpty()) {
            statusMap = valueList.get(0);
            statusValueMap.put("draft", statusMap.get("doc_count"));
            statusValueMap.put("draftBucket", statusMap.get("createdOn"));
          }
        } else if (data.containsKey("Publish")) {
          statusMap = (Map<String, Object>) data.get("Publish");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if (!valueList.isEmpty()) {
            statusMap = valueList.get(0);
            statusValueMap.put("live", statusMap.get("doc_count"));
            statusValueMap.put("liveBucket", statusMap.get("lastPublishedOn"));
          }
        } else if (data.containsKey("Review")) {
          statusMap = (Map<String, Object>) data.get("Review");
          valueList = (List<Map<String, Object>>) statusMap.get("buckets");
          if (!valueList.isEmpty()) {
            statusMap = valueList.get(0);
            statusValueMap.put("review", statusMap.get("doc_count"));
            statusValueMap.put("reviewBucket", statusMap.get("lastSubmittedOn"));
          }
        }
      }

      dataMap.put(JsonKey.NAME, "Number of content items created");
      value = statusValueMap.get("draft");
      dataMap.put(VALUE, value);
      snapshot.put("org.creation.content[@status=draft].count", dataMap);
      dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of content items reviewed");
      value = statusValueMap.get("review");
      dataMap.put(VALUE, value);
      snapshot.put("org.creation.content[@status=review].count", dataMap);
      dataMap = new HashMap<>();
      value = statusValueMap.get("live");
      dataMap.put(JsonKey.NAME, "Number of content items published");
      dataMap.put(VALUE, value);
      snapshot.put("org.creation.content[@status=published].count", dataMap);

      Map<String, Object> series = new LinkedHashMap<>();
      Map aggKeyMap = (Map) resultData.get("createdOn");
      List<Map<String, Object>> bucket = getBucketData(aggKeyMap, periodStr);
      Map<String, Object> seriesData = new LinkedHashMap<>();
    /*  if ("5w".equalsIgnoreCase(periodStr)) {
        seriesData.put(JsonKey.NAME, "Content created per week");
      } else {
        seriesData.put(JsonKey.NAME, "Content created per day");
      }
      seriesData.put(JsonKey.SPLIT, "content.created_on");
      seriesData.put(GROUP_ID, "org.content.count");
      if (null == bucket || bucket.isEmpty()) {
        bucket = createBucketStructure(periodStr);
      }
      seriesData.put("buckets", bucket);
      series.put("org.creation.content.created_on.count", seriesData);*/

      Map<String, Object> statusList = new HashMap();
      List<Map<String, Object>> statusBucket = new ArrayList<>();
      statusList = (Map<String, Object>) statusValueMap.get("draftBucket");
      statusBucket = getBucketData(statusList, periodStr);
      if (null == statusBucket || statusBucket.isEmpty()) {
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Draft");
      seriesData.put(JsonKey.SPLIT, "content.created_on");
      seriesData.put(GROUP_ID, "org.content.count");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=draft].count", seriesData);

      statusList = (Map<String, Object>) statusValueMap.get("reviewBucket");
      statusBucket = getBucketData(statusList, periodStr);
      if (null == statusBucket || statusBucket.isEmpty()) {
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Review");
      seriesData.put(JsonKey.SPLIT, "content.reviewed_on");
      seriesData.put(GROUP_ID, "org.content.count");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=review].count", seriesData);

      statusList = (Map<String, Object>) statusValueMap.get("liveBucket");
      statusBucket = getBucketData(statusList, periodStr);
      if (null == statusBucket || statusBucket.isEmpty()) {
        statusBucket = createBucketStructure(periodStr);
      }
      seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Live");
      seriesData.put(JsonKey.SPLIT, "content.published_on");
      seriesData.put(GROUP_ID, "org.content.count");
      seriesData.put("buckets", statusBucket);
      series.put("org.creation.content[@status=published].count", seriesData);

      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);

      result = mapper.writeValueAsString(responseMap);
    } catch (JsonProcessingException e) {
      ProjectLogger.log("Error occured", e);
    }
    return result;
  }


  private void orgCreationMetrics(Request actorMessage) {
    ProjectLogger.log("In orgCreationMetrics api");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      Map<String, Object> orgData = validateOrg(orgId);
      if (null == orgData) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
      String orgName = (String) orgData.get(JsonKey.ORG_NAME);
      if (ProjectUtil.isStringNullOREmpty(orgName)) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
      Map<String, Object> aggregationMap = new HashMap<>();
      for (String operation : operationList) {
        String request = getQueryRequest(periodStr, orgId, operation);
        String esResponse = makePostRequest(JsonKey.EKSTEP_ES_METRICS_API_URL, request);
        // String esResponse = getDataFromEkstep(request, JsonKey.EKSTEP_ES_METRICS_URL);
        aggregationMap = putAggregationMap(esResponse, aggregationMap, operation);
      }
      String responseFormat = orgCreationResponseGenerator(periodStr, aggregationMap);
      Response response =
          metricsResponseGenerator(responseFormat, periodStr, getViewData(orgId, orgName));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs", e);
      sender().tell(e, self());
      return;
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs", e);
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private void orgConsumptionMetrics(Request actorMessage) {
    ProjectLogger.log("In orgConsumptionMetrics api");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      Map<String, Object> orgData = validateOrg(orgId);
      if (null == orgData) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
      String orgName = (String) orgData.get(JsonKey.ORG_NAME);
      String orgHashId = (String) orgData.get(JsonKey.HASHTAGID);
      ProjectLogger.log("orgId" + orgHashId);
      if (ProjectUtil.isStringNullOREmpty(orgName)) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
      if(ProjectUtil.isStringNullOREmpty(orgHashId)){
        orgHashId = orgId;
      }
      String orgRootId = (String)orgData.get(JsonKey.ROOT_ORG_ID);
      if(ProjectUtil.isStringNullOREmpty(orgRootId)){
        orgRootId = orgId;
      }
      ProjectLogger.log("RootOrgId " + orgRootId);
      Map<String, Object> rootOrgData = validateOrg(orgRootId);
      if(null == rootOrgData || rootOrgData.isEmpty()){
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRootOrgData.getErrorCode(),
                ResponseCode.invalidRootOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }
      String channel = (String) rootOrgData.get(JsonKey.HASHTAGID);
      ProjectLogger.log("channel" + channel);
      String requestStr = getOrgMetricsRequest(actorMessage, periodStr, orgHashId, null, channel);
      String ekStepResponse = makePostRequest(JsonKey.EKSTEP_METRICS_API_URL, requestStr);
      String responseFormat = orgConsumptionResponseGenerator(periodStr, ekStepResponse);
      Response response =
          metricsResponseGenerator(responseFormat, periodStr, getViewData(orgId, orgName));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs", e);
      sender().tell(e, self());
      return;
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs", e);
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private String getOrgMetricsRequest(Request actorMessage, String periodStr, String orgHashId,
      String userId, String channel) throws JsonProcessingException {
    Request request = new Request();
    request.setId(actorMessage.getId());
    Map<String, Object> requestObject = new HashMap<>();
    requestObject.put(JsonKey.PERIOD, getEkstepPeriod(periodStr));
    Map<String, Object> filterMap = new HashMap<>();
    filterMap.put(JsonKey.TAG, orgHashId);
    if (!ProjectUtil.isStringNullOREmpty(userId)) {
      filterMap.put(USER_ID, userId);
    }
    requestObject.put(JsonKey.FILTER, filterMap);
    requestObject.put(JsonKey.CHANNEL, channel);
    request.setRequest(requestObject);
    String requestStr = mapper.writeValueAsString(request);
    return requestStr;
  }

  @SuppressWarnings("unchecked")
  private String orgConsumptionResponseGenerator(String period, String ekstepResponse) {
    String result = "";
    try {
      Map<String, Object> resultData = mapper.readValue(ekstepResponse, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      List<Map<String, Object>> resultList =
          (List<Map<String, Object>>) resultData.get(JsonKey.METRICS);
      List<Map<String, Object>> buckets = createBucketStructure(period);
      List<Map<String, Object>> userBucket = new ArrayList<>();
      List<Map<String, Object>> consumptionBucket = new ArrayList<>();
      Map<String, Object> userData = new HashMap<>();
      int index = 0;
      Collections.reverse(resultList);
      Map<String, Object> resData = new HashMap<>();
      for (Map<String, Object> res : resultList) {
        resData = buckets.get(index);
        userData = resData;
        String bucketDate = "";
        String metricsDate = "";
        if ("5w".equalsIgnoreCase(period)) {
          bucketDate = (String) resData.get("key");
          bucketDate = bucketDate.substring(bucketDate.length() - 2, bucketDate.length());
          metricsDate = String.valueOf(res.get("d_period"));
          metricsDate = metricsDate.substring(metricsDate.length() - 2, metricsDate.length());
        } else {
          bucketDate = (String) resData.get("key_name");
          metricsDate = String.valueOf(res.get("d_period"));
          Date date = new SimpleDateFormat("yyyyMMdd").parse(metricsDate);
          metricsDate = new SimpleDateFormat("yyyy-MM-dd").format(date);
        }
        if (metricsDate.equalsIgnoreCase(bucketDate)) {
          Double totalTimeSpent = (Double) res.get("m_total_ts");
          Integer totalUsers = (Integer) res.get("m_total_users_count");
          resData.put(VALUE, totalTimeSpent);
          userData.put(VALUE, totalUsers);
        }
        consumptionBucket.add(resData);
        userBucket.add(userData);
        if (index < buckets.size()) {
          index++;
        }
      }

      Map<String, Object> series = new HashMap<>();

      Map<String, Object> seriesData = new LinkedHashMap<>();
      if ("5w".equalsIgnoreCase(period)) {
        seriesData.put(JsonKey.NAME, "Time spent by week");
      } else {
        seriesData.put(JsonKey.NAME, "Time spent by day");
      }
      seriesData.put(JsonKey.SPLIT, "content.time_spent.user.count");
      seriesData.put(JsonKey.TIME_UNIT, "seconds");
      seriesData.put(GROUP_ID, "org.timespent.sum");
      seriesData.put("buckets", consumptionBucket);
      series.put("org.consumption.content.time_spent.sum", seriesData);
      seriesData = new LinkedHashMap<>();
      if ("5w".equalsIgnoreCase(period)) {
        seriesData.put(JsonKey.NAME, "Number of users per week");
      } else {
        seriesData.put(JsonKey.NAME, "Number of users per day");
      }
      seriesData.put(JsonKey.SPLIT, "content.users.count");
      seriesData.put(GROUP_ID, "org.users.count");
      seriesData.put("buckets", userBucket);
      series.put("org.consumption.content.users.count", seriesData);

      resultData = (Map<String, Object>) resultData.get(JsonKey.SUMMARY);
      Map<String, Object> snapshot = new LinkedHashMap<>();
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Number of visits by users");
      dataMap.put(VALUE, resultData.get("m_total_users_count"));
      snapshot.put("org.consumption.content.session.count", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Content consumption time");
      dataMap.put(VALUE, resultData.get("m_total_ts"));
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("org.consumption.content.time_spent.sum", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Average time spent by user per visit");
      dataMap.put(VALUE, resultData.get("m_avg_ts_session"));
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("org.consumption.content.time_spent.average", dataMap);
      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);

      result = mapper.writeValueAsString(responseMap);
    } catch (JsonProcessingException e) {
      ProjectLogger.log("Error occured",e);
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
    }
    return result;
  }

  private Map<String, Object> validateOrg(String orgId) {
    try {
      Map<String, Object> result =
          ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(), orgId);
      if (null == result || result.isEmpty()) {
        return null;
      }
      ProjectLogger.log("Result:"+result.toString());
      return result;
    } catch (Exception e) {
      ProjectLogger.log("Error occured",e);
      throw new ProjectCommonException(ResponseCode.esError.getErrorCode(),
          ResponseCode.esError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private void orgCreationMetricsData(Request actorMessage) {
    ProjectLogger.log("In orgCreationMetricsExcel api");
    try {
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      List<Object> headers = new ArrayList<>();
      headers.add("contentCreatedFor");
      headers.add("userId");
      headers.add("userName");
      headers.add("userCreatedOn");
      headers.add("contentName");
      headers.add("contentType");
      headers.add("contentLanguage");
      headers.add("contentSize");
      headers.add("contentCreatedOn");
      headers.add("contentLastPublishedOn");
      headers.add("contentReviewedOn");
      headers.add("contentLastUpdatedOn");
      headers.add("contentLastUpdatedStatus");
      headers.add("contentLastPublishedBy");
      headers.add("contentConceptsCovered");
      headers.add("contentDomain");
      headers.add("contentTagsCount");
      headers.add("contentCreationTimeSpent");
      headers.add("contentCreationTotalSessions");
      headers.add("contentCreationAvgTimePerSession");
      List<List<Object>> csvRecords = new ArrayList<>();
      csvRecords.add(headers);
      for (String operation : operationList) {
        String requestStr = getRequestObject(operation, actorMessage);
        String ekStepResponse = makePostRequest(JsonKey.EKSTEP_CONTNET_SEARCH_URL, requestStr);
        List<Map<String, Object>> ekstepData = getDataFromResponse(ekStepResponse, headers, orgId);
        List<Map<String, Object>> userData = getUserDetailsFromES(ekstepData);
        csvRecords.addAll(generateDataList(userData, headers));
      }
      String period = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String fileName =
          "CreationReport_" + orgId + FILENAMESEPARATOR + System.currentTimeMillis() + FILENAMESEPARATOR + period;      
      String requestId = (String) actorMessage.getRequestId();
      saveData(csvRecords, requestId);
      Request backGroundRequest = new Request();
      backGroundRequest.setOperation(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue());

      Map<String , Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.REQUEST_ID , requestId);
      innerMap.put(JsonKey.FILE_NAME, fileName);
      
      backGroundRequest.setRequest(innerMap);
      backGroundActorRef.tell(backGroundRequest , self());  
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs", e);
      //TODO:
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    return;
  }

  @SuppressWarnings("unchecked")
  private void orgConsumptionMetricsData(Request actorMessage) {
    ProjectLogger.log("In orgConsumptionMetricsExcel api");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
      Map<String, Object> orgData = validateOrg(orgId);
      if (null == orgData) {
        throw new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                ResponseCode.invalidOrgData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      String orgHashId = (String) orgData.get(JsonKey.HASHTAGID);
      String channel =
          (String) (orgData.get(JsonKey.CHANNEL) == null ? "" : orgData.get(JsonKey.CHANNEL));

      List<Object> headers = new ArrayList<>();
      headers.add("userId");
      headers.add("userName");
      headers.add("userCreatedOn");
      headers.add("totalNumberOfVisitsByUser");
      headers.add("totalTimeSpentOnConsumingContent");
      headers.add("totalPiecesOfContentConsumed");
      headers.add("avgTimeSpentPerVisit");
      List<Map<String, Object>> consumptionData = new ArrayList<>();
      List<Map<String, Object>> usersData = getUserDetailsUsingOrg(orgId);
      for (Map<String, Object> userData : usersData) {
        String request = getOrgMetricsRequest(actorMessage, periodStr, orgHashId,
            (String) userData.get(JsonKey.ID), channel);
        String esResponse = makePostRequest(JsonKey.EKSTEP_METRICS_API_URL, request);
        Map<String, Object> ekstepData =
            getConsumptionDataFromResponse(esResponse, userData, (List<String>) (Object) headers);
        consumptionData.add(ekstepData);
      }
      List<List<Object>> csvRecords = new ArrayList<>();
      csvRecords.add(headers);
      csvRecords.addAll(generateDataList(consumptionData, headers));
      
      String requestId = (String) actorMessage.getRequestId();
      String fileName =
          "ConsumptionReport" + orgId + FILENAMESEPARATOR + System.currentTimeMillis() + FILENAMESEPARATOR + periodStr;      
      saveData(csvRecords, requestId);
      Request backGroundRequest = new Request();
      backGroundRequest.setOperation(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue());

      Map<String , Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.REQUEST_ID , requestId);
      innerMap.put(JsonKey.FILE_NAME, fileName);
      
      backGroundRequest.setRequest(innerMap);
      backGroundActorRef.tell(backGroundRequest , self());
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs", e);
      //TODO:
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    return;
  }

  private List<List<Object>> generateDataList(List<Map<String, Object>> aggregationMap,
      List<Object> headers) {
    List<List<Object>> result = new ArrayList<>();
    for (Map<String, Object> data : aggregationMap) {
      List<Object> dataResult = new ArrayList<>();
      for (Object header : headers) {
        dataResult.add(data.get(header));
      }
      result.add(dataResult);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUserDetailsFromES(List<Map<String, Object>> ekstepData) {
    List<String> coursefields = new ArrayList<>();
    List<Map<String, Object>> userResult = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.USER_NAME);
    coursefields.add(JsonKey.CREATED_DATE);
    String userId = "";
    for (Map<String, Object> userData : ekstepData) {
      Map<String, Object> data = new HashMap<>();
      if (userData.containsKey("userId")) {
        userId = (String) userData.get("userId");
      } else {
        return ekstepData;
      }
      Map<String, Object> filter = new HashMap<>();
      filter.put(JsonKey.IDENTIFIER, userId);
      try {
        Map<String, Object> result =
            ElasticSearchUtil.complexSearch(createESRequest(filter, null, coursefields),
                ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
        if (null != result && !result.isEmpty()) {
          List<Map<String, Object>> resultList =
              (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
          if (null != resultList && !resultList.isEmpty()) {
            for (Map<String, Object> dataObject : resultList) {
              data.putAll(dataObject);
            }
          }
          data.putAll(userData);
          userResult.add(data);
        }
      } catch (Exception e) {
        throw new ProjectCommonException(ResponseCode.esError.getErrorCode(),
            ResponseCode.esError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
      }

    }
    return userResult;
  }


  private String getRequestObject(String operation, Request actorMessage) {
    Request request = new Request();

    String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    Map<String, Object> dateMap = getStartAndEndDate(periodStr);
    Map<String, String> operationMap = new LinkedHashMap<>();
    switch (operation) {
      case "Create": {
        operationMap.put("dateKey", "createdOn");
        operationMap.put("status", ContentStatus.Draft.name());
        break;
      }
      case "Review": {
        operationMap.put("dateKey", "lastSubmittedOn");
        operationMap.put("status", ContentStatus.Review.name());
        break;
      }
      case "Publish": {
        operationMap.put("dateKey", "lastPublishedOn");
        operationMap.put("status", ContentStatus.Live.name());
        break;
      }
    }
    Map<String, Object> requestObject = new HashMap<>();
    Map<String, Object> filterMap = new HashMap<>();
    List<String> fields = new ArrayList<>();
    Map<String, Object> dateValue = new HashMap<>();
    dateValue.put("min", dateMap.get(startDate));
    dateValue.put("max", dateMap.get(endDate));
    filterMap.put(operationMap.get("dateKey"), dateValue);
    List<String> contentType = new ArrayList<>();
    contentType.add("Story");
    contentType.add("Worksheet");
    contentType.add("Game");
    contentType.add("Collection");
    contentType.add("TextBook");
    contentType.add("TextBookUnit");
    contentType.add("Course");
    contentType.add("CourseUnit");
    filterMap.put("contentType", contentType);
    filterMap.put("createdFor", orgId);
    filterMap.put(JsonKey.STATUS, operationMap.get(JsonKey.STATUS));
    requestObject.put(JsonKey.FILTERS, filterMap);
    fields.add("createdBy");
    fields.add("createdFor");
    fields.add("createdOn");
    fields.add("lastUpdatedOn");
    fields.add("status");
    fields.add("lastPublishedBy");
    fields.add("name");
    fields.add("contentType");
    fields.add("size");
    fields.add("concepts");
    fields.add("domain");
    fields.add("tags");
    fields.add("language");
    fields.add("lastPublishedOn");
    fields.add("lastSubmittedOn");
    fields.add("me_creationTimespent");
    fields.add("me_creationSessions");
    fields.add("me_avgCreationTsPerSession");
    requestObject.put(JsonKey.FIELDS, fields);
    request.setRequest(requestObject);
    String requestStr = "";
    try {
      requestStr = mapper.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      ProjectLogger.log("Error occured", e);
    }
    return requestStr;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getDataFromResponse(String responseData, List<Object> headers, String orgId) {
    List<Map<String, Object>> result = new ArrayList<>();
    try {
      Map<String, Object> resultData = mapper.readValue(responseData, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      List<Map<String, Object>> resultList =
          (List<Map<String, Object>>) resultData.get(JsonKey.CONTENT);
      for (Map<String, Object> content : resultList) {
        Map<String, Object> data = new HashMap<>();
        for (String header : content.keySet()) {
          String headerName = StringUtils.capitalize(header);
          headerName = "content" + headerName;
          if (headers.contains(headerName)) {
            data.put(headerName, content.get(header));
          } else if ("status".equalsIgnoreCase(header)) {
            data.put("contentLastUpdatedStatus", content.get(header));
          } else if ("contentType".equalsIgnoreCase(header)) {
            data.put(header, content.get(header));
          } else if ("concepts".equalsIgnoreCase(header)) {
            List<String> concepts = getConcepts((List<String>) content.get(header));
            data.put("contentConceptsCovered", concepts);
          } else if ("tags".equalsIgnoreCase(header)) {
            List<String> tags = (List<String>) content.get(header);
            data.put("contentTagsCount", tags.size());
          }else if("lastSubmittedOn".equals(header)){
            data.put("contentReviewedOn", content.get(header));
          }
          if("createdFor".equalsIgnoreCase(header)){
            data.put(headerName, orgId);
          }
          if ("createdBy".equalsIgnoreCase(header) || "lastPublishedBy".equalsIgnoreCase(header)) {
            data.put("userId", content.get(header));
          }
          
        }
        result.add(data);
      }
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
    }
    return result;
  }

  private static List<String> getConcepts(List<String> data) {
    List<String> result = new ArrayList<>();
    for (String concept : data) {
      String conceptName = getConcept(concept);
      result.add(conceptName);
    }
    return result;
  }

  private static String getConcept(String data) {
    if (null == conceptsList || conceptsList.isEmpty()) {
      conceptList();
    }
    return conceptsList.get(data);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> conceptList() {
    List<String> domains = new ArrayList<>();
    domains.add("numeracy");
    domains.add("literacy");
    domains.add("science");

    for (String domain : domains) {
      String url = PropertiesCache.getInstance().getProperty((JsonKey.EKSTEP_CONCEPT_URL));
      url = StringUtils.replace(url, "{domain}", domain);
      String resposne = getDataFromEkstep(url);
      try {
        Map<String, Object> responseMap = mapper.readValue(resposne, Map.class);
        responseMap = (Map<String, Object>) responseMap.get(JsonKey.RESULT);
        List<Map<String, Object>> conceptData =
            (List<Map<String, Object>>) responseMap.get("concepts");
        for (Map<String, Object> concept : conceptData) {
          String id = (String) concept.get(JsonKey.IDENTIFIER);
          String name = (String) concept.get(JsonKey.NAME);
          conceptsList.put(id, name);
        }
      } catch (IOException e) {
        ProjectLogger.log("Error occured", e);
      }
    }
    return conceptsList;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getConsumptionDataFromResponse(String responseData,
      Map<String, Object> userData, List<String> headers) {
    Map<String, Object> resultMap = new HashMap<>();
    try {
      Map<String, Object> resultData = mapper.readValue(responseData, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      Map<String, Object> result = (Map<String, Object>) resultData.get(JsonKey.SUMMARY);
      resultMap.put(headers.get(0), userData.get(JsonKey.ID));
      resultMap.put(headers.get(1), userData.get(JsonKey.USERNAME));
      resultMap.put(headers.get(2), userData.get(JsonKey.CREATED_DATE));
      resultMap.put(headers.get(3), result.get("m_total_sessions"));
      resultMap.put(headers.get(4), result.get("m_total_ts"));
      resultMap.put(headers.get(5), result.get("m_total_content_count"));
      resultMap.put(headers.get(6), result.get("m_avg_ts_session"));
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
    }
    return resultMap;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUserDetailsUsingOrg(String orgId) {
    List<String> coursefields = new ArrayList<>();
    List<Map<String, Object>> userResult = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.USER_NAME);
    coursefields.add(JsonKey.CREATED_DATE);
    Map<String, Object> filter = new HashMap<>();
    filter.put("organisations.organisationId", orgId);
    try {
      Map<String, Object> result =
          ElasticSearchUtil.complexSearch(createESRequest(filter, null, coursefields),
              ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
      if (null != result && !result.isEmpty()) {
        List<Map<String, Object>> resultList =
            (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
        if (null != resultList && !resultList.isEmpty()) {
          for (Map<String, Object> dataObject : resultList) {
            userResult.add(dataObject);
          }
        }
      }
      return userResult;
    } catch (Exception e) {
      throw new ProjectCommonException(ResponseCode.esError.getErrorCode(),
          ResponseCode.esError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }
}
