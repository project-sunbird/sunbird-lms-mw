/**
 * 
 */
package org.sunbird.learner.actors.badges;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.Util;

/**
 * @author Manzarul
 *
 */
public class BadgesActor extends UntypedAbstractActor {



  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo badgesDbInfo = Util.dbInfoMap.get(JsonKey.BADGES_DB);
  private Util.DbInfo userBadgesDbInfo = Util.dbInfoMap.get(JsonKey.USER_BADGES_DB);

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("AssessmentItemActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_ALL_BADGE.getValue())) {
          getBadges(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ADD_USER_BADGE.getValue())) {
          saveUserBadges(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.HEALTH_CHECK.getValue())) {
          checkAllComponentHealth(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ACTOR.getValue())) {
          actorhealthCheck(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ES.getValue())) {
          esHealthCheck(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.CASSANDRA.getValue())) {
          cassandraHealthCheck(actorMessage);
        }

        else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
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

  /**
   * @param actorMessage
   */
  private void cassandraHealthCheck(Request actorMessage) {
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

  /**
   * @param actorMessage
   */
  private void esHealthCheck(Request actorMessage) {
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

  /**
   * @param actorMessage
   */
  private void actorhealthCheck(Request actorMessage) {
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

  /**
   * 
   */
  private void checkAllComponentHealth(Request actorMessage) {
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
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, response, null));
      isallHealthy = response;
    } catch (Exception e) {
      responseList.add(ProjectUtil.createCheckResponse(JsonKey.ES_SERVICE, true, e));
      isallHealthy = false;
    }
    // check EKStep Util.
    try {
      String body = "{\"request\":{\"filters\":{\"identifier\":\"test\"}}}";
      Map<String, String> headers = new HashMap<>();
      headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
      if (ProjectUtil.isStringNullOREmpty((String) headers.get(JsonKey.AUTHORIZATION))) {
        headers.put(JsonKey.AUTHORIZATION,
            PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
        headers.put("Content_Type", "application/json; charset=utf-8");
      }
      String response = HttpUtil.sendPostRequest(
          PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL)
              + PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_URL),
          body, headers);
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
    Response assmntResponse = new Response();
    String receiverId = (String) req.get(JsonKey.RECEIVER_ID);
    String badgeTypeId = (String) req.get(JsonKey.BADGE_TYPE_ID);
    Map<String, Object> map =
        ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(), receiverId);
    if (map == null || map.size() == 0) {
      ProjectCommonException ex =
          new ProjectCommonException(ResponseCode.invalidReceiverId.getErrorCode(),
              ResponseCode.invalidReceiverId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(ex, self());
      return;
    }

    Response response = cassandraOperation.getRecordById(badgesDbInfo.getKeySpace(),
        badgesDbInfo.getTableName(), badgeTypeId);
    if (response != null && response.get(JsonKey.RESPONSE) != null) {
      List<Map<String, Object>> badgesListMap =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (badgesListMap == null || badgesListMap.size() == 0) {
        ProjectCommonException ex =
            new ProjectCommonException(ResponseCode.invalidBadgeTypeId.getErrorCode(),
                ResponseCode.invalidBadgeTypeId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(ex, self());
        return;
      }
    } else {
      ProjectCommonException ex =
          new ProjectCommonException(ResponseCode.invalidBadgeTypeId.getErrorCode(),
              ResponseCode.invalidBadgeTypeId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(ex, self());
      return;
    }
    try {
      map = new HashMap<>();
      map.put(JsonKey.RECEIVER_ID, receiverId);
      map.put(JsonKey.BADGE_TYPE_ID, (String) req.get(JsonKey.BADGE_TYPE_ID));
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
      map.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      map.put(JsonKey.CREATED_BY, (String) req.get(JsonKey.REQUESTED_BY));
      cassandraOperation.insertRecord(userBadgesDbInfo.getKeySpace(),
          userBadgesDbInfo.getTableName(), map);
      assmntResponse.put(JsonKey.ID, map.get(JsonKey.ID));
      assmntResponse.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    } catch (Exception e) {
      assmntResponse.put(JsonKey.RESPONSE, JsonKey.FAILURE);
      sender().tell(assmntResponse, self());
    }
    sender().tell(assmntResponse, self());
    try {
      ProjectLogger.log("Start background job to save user badge.");
      Request request = new Request();
      request.setOperation(ActorOperations.ADD_USER_BADGE_BKG.getValue());
      request.getRequest().put(JsonKey.RECEIVER_ID, receiverId);
      ActorUtil.tell(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occured during saving user badges to ES : ", ex);
    }
  }


  @SuppressWarnings("unchecked")
  private void getBadges(Request actorMessage) {
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
