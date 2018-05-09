/** */
package org.sunbird.learner.actors.health;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;

/** @author Manzarul */
@ActorConfig(
  tasks = {"getAllBadge", "addUserBadge", "healthCheck", "actor", "es", "cassandra"},
  asyncTasks = {}
)
public class HealthActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo badgesDbInfo = Util.dbInfoMap.get(JsonKey.BADGES_DB);
  private Util.DbInfo userBadgesDbInfo = Util.dbInfoMap.get(JsonKey.USER_BADGES_DB);

  @Override
  public void onReceive(Request message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("AssessmentItemActor onReceive called");
        Request actorMessage = message;
        Util.initializeContext(actorMessage, JsonKey.USER);
        // set request id fto thread loacl...
        ExecutionContext.setRequestId(actorMessage.getRequestId());
        // TODO need to remove the badges api's
        if (actorMessage
            .getOperation()
            .equalsIgnoreCase(ActorOperations.GET_ALL_BADGE.getValue())) {
          getBadges();
        } else if (actorMessage
            .getOperation()
            .equalsIgnoreCase(ActorOperations.ADD_USER_BADGE.getValue())) {
          saveUserBadges(actorMessage);
        } else if (actorMessage
            .getOperation()
            .equalsIgnoreCase(ActorOperations.HEALTH_CHECK.getValue())) {
          checkAllComponentHealth();
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ACTOR.getValue())) {
          actorhealthCheck();
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ES.getValue())) {
          esHealthCheck();
        } else if (actorMessage
            .getOperation()
            .equalsIgnoreCase(ActorOperations.CASSANDRA.getValue())) {
          cassandraHealthCheck();
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception =
              new ProjectCommonException(
                  ResponseCode.invalidOperationName.getErrorCode(),
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
          new ProjectCommonException(
              ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  /** */
  private void esHealthCheck() {
    // check the elastic search
    boolean isallHealthy = true;
    Map<String, Object> finalResponseMap = new HashMap<>();
    List<Map<String, Object>> responseList = new ArrayList<>();
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.ACTOR_SERVICE, false, null));
    try {
      boolean esResponse = ElasticSearchUtil.healthCheck();
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, esResponse, null));
      isallHealthy = esResponse;
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, true, e));
      isallHealthy = false;
      ProjectLogger.log("Elastic search health Error == ", e);
    }
    finalResponseMap.put(JsonKey.CHECKS, responseList);
    finalResponseMap.put(JsonKey.NAME, "ES health check api");
    if (isallHealthy) {
      finalResponseMap.put(JsonKey.Healthy, true);
    } else {
      finalResponseMap.put(JsonKey.Healthy, false);
    }
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, finalResponseMap);
    sender().tell(response, self());
  }

  /** */
  private void cassandraHealthCheck() {
    Map<String, Object> finalResponseMap = new HashMap<>();
    List<Map<String, Object>> responseList = new ArrayList<>();
    boolean isallHealthy = false;
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.LEARNER_SERVICE, false, null));
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.ACTOR_SERVICE, false, null));
    try {
      cassandraOperation.getAllRecords(badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName());
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.CASSANDRA_SERVICE, false, null));
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.CASSANDRA_SERVICE, true, e));
      isallHealthy = false;
    }
    finalResponseMap.put(JsonKey.CHECKS, responseList);
    finalResponseMap.put(JsonKey.NAME, "cassandra health check api");
    if (isallHealthy) {
      finalResponseMap.put(JsonKey.Healthy, true);
    } else {
      finalResponseMap.put(JsonKey.Healthy, false);
    }
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, finalResponseMap);
    sender().tell(response, self());
  }

  /** */
  private void actorhealthCheck() {
    Map<String, Object> finalResponseMap = new HashMap<>();
    List<Map<String, Object>> responseList = new ArrayList<>();
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.LEARNER_SERVICE, false, null));
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.ACTOR_SERVICE, false, null));
    finalResponseMap.put(JsonKey.CHECKS, responseList);
    finalResponseMap.put(JsonKey.NAME, "Actor health check api");
    finalResponseMap.put(JsonKey.Healthy, true);
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, finalResponseMap);
    sender().tell(response, self());
  }

  /** */
  private void checkAllComponentHealth() {
    boolean isallHealthy = true;
    Map<String, Object> finalResponseMap = new HashMap<>();
    List<Map<String, Object>> responseList = new ArrayList<>();
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.LEARNER_SERVICE, false, null));
    responseList.add(ProjectUtil.createCheckResponse(JsonKey.ACTOR_SERVICE, false, null));
    try {
      cassandraOperation.getAllRecords(badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName());
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.CASSANDRA_SERVICE, false, null));
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.CASSANDRA_SERVICE, true, e));
      isallHealthy = false;
    }
    // check the elastic search
    try {
      boolean response = ElasticSearchUtil.healthCheck();
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, !response, null));
      isallHealthy = response;
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, true, e));
      isallHealthy = false;
    }
    // check EKStep Util.
    try {
      String body = "{\"request\":{\"filters\":{\"identifier\":\"test\"}}}";
      Map<String, String> headers = new HashMap<>();
      headers.put(
          JsonKey.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.EKSTEP_AUTHORIZATION));
      if (StringUtils.isBlank(headers.get(JsonKey.AUTHORIZATION))) {
        headers.put(
            JsonKey.AUTHORIZATION,
            PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
        headers.put("Content_Type", "application/json; charset=utf-8");
      }
      String ekStepBaseUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
      if (StringUtils.isBlank(ekStepBaseUrl)) {
        ekStepBaseUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL);
      }
      String response =
          HttpUtil.sendPostRequest(
              ekStepBaseUrl
                  + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_URL),
              body,
              headers);
      if (response.contains("OK")) {
        responseList.add(ProjectUtil.createCheckResponse(JsonKey.EKSTEP_SERVICE, false, null));
      } else {
        responseList.add(ProjectUtil.createCheckResponse(JsonKey.EKSTEP_SERVICE, true, null));
      }
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.EKSTEP_SERVICE, true, null));
      isallHealthy = false;
    }

    finalResponseMap.put(JsonKey.CHECKS, responseList);
    finalResponseMap.put(JsonKey.NAME, "Complete health check api");
    if (isallHealthy) {
      finalResponseMap.put(JsonKey.Healthy, true);
    } else {
      finalResponseMap.put(JsonKey.Healthy, false);
    }
    Response response = new Response();
    response.getResult().put(JsonKey.RESPONSE, finalResponseMap);
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void saveUserBadges(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> targetObject = null;
    // correlated object of telemetry event...
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String receiverId = (String) req.get(JsonKey.RECEIVER_ID);
    // Telemetry target object
    targetObject =
        TelemetryUtil.generateTargetObject(receiverId, JsonKey.USER, JsonKey.CREATE, null);
    String badgeTypeId = (String) req.get(JsonKey.BADGE_TYPE_ID);
    // correlated object for telemetry...
    TelemetryUtil.generateCorrelatedObject(badgeTypeId, "badge", "user.badge", correlatedObject);

    Map<String, Object> map =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            receiverId);
    if (map == null || map.size() == 0) {
      ProjectCommonException ex =
          new ProjectCommonException(
              ResponseCode.invalidReceiverId.getErrorCode(),
              ResponseCode.invalidReceiverId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(ex, self());
      return;
    }

    Response response =
        cassandraOperation.getRecordById(
            badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName(), badgeTypeId);
    if (response != null && response.get(JsonKey.RESPONSE) != null) {
      List<Map<String, Object>> badgesListMap =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (badgesListMap == null || badgesListMap.isEmpty()) {
        ProjectCommonException ex =
            new ProjectCommonException(
                ResponseCode.invalidBadgeTypeId.getErrorCode(),
                ResponseCode.invalidBadgeTypeId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(ex, self());
        return;
      }
    } else {
      ProjectCommonException ex =
          new ProjectCommonException(
              ResponseCode.invalidBadgeTypeId.getErrorCode(),
              ResponseCode.invalidBadgeTypeId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(ex, self());
      return;
    }
    Response result = new Response();
    try {
      map = new HashMap<>();
      map.put(JsonKey.RECEIVER_ID, receiverId);
      map.put(JsonKey.BADGE_TYPE_ID, req.get(JsonKey.BADGE_TYPE_ID));
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
      map.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      map.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
      cassandraOperation.insertRecord(
          userBadgesDbInfo.getKeySpace(), userBadgesDbInfo.getTableName(), map);
      result.put(JsonKey.ID, map.get(JsonKey.ID));
      result.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    } catch (Exception e) {
      result.put(JsonKey.RESPONSE, JsonKey.FAILURE);
      sender().tell(result, self());
    }
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
    sender().tell(result, self());
    try {
      ProjectLogger.log("Start background job to save user badge.");
      Request request = new Request();
      request.setOperation(ActorOperations.ADD_USER_BADGE_BKG.getValue());
      request.getRequest().put(JsonKey.RECEIVER_ID, receiverId);
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user badges to ES : ", ex);
    }
  }

  private void getBadges() {
    try {
      Response response =
          cassandraOperation.getAllRecords(badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName());
      sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      sender().tell(e, self());
    }
  }
}
