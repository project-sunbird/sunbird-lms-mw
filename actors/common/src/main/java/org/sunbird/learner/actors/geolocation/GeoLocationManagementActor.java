package org.sunbird.learner.actors.geolocation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.models.util.fcm.Notification;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.geolocation.dao.GeolocationDao;
import org.sunbird.learner.actors.geolocation.dao.impl.GeolocationDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.geolocation.Geolocation;
import org.sunbird.telemetry.util.TelemetryUtil;

/** Class for providing Geo Location for Organisation Created by arvind on 31/10/17. */
@ActorConfig(
  tasks = {
    "getGeoLocation",
    "createGeoLocation",
    "updateGeoLocation",
    "deleteGeoLocation",
    "sendNotification",
    "getUserCount"
  },
  asyncTasks = {}
)
public class GeoLocationManagementActor extends BaseActor {

  private Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  private GeolocationDao geolocationDao = GeolocationDaoImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    ExecutionContext.setRequestId(request.getRequestId());
    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    String operation = request.getOperation();

    switch (operation) {
      case "createGeoLocation":
        createGeoLocation(request);
        break;
      case "getGeoLocation":
        getGeoLocation(request);
        break;
      case "deleteGeoLocation":
        deleteGeoLocation(request);
        break;
      case "updateGeoLocation":
        updateGeoLocation(request);
        break;
      case "getUserCount":
        getUserCount(request);
        break;
      case "sendNotification":
        sendNotification(request);
        break;
      default:
        onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void getUserCount(Request actorMessage) {
    ProjectLogger.log("sendnotification actor method called.");
    List<Object> locationIds = (List<Object>) actorMessage.getRequest().get(JsonKey.LOCATION_IDS);
    List<Map<String, Object>> result = new ArrayList<>();
    List<String> dbIdList = new ArrayList<>();
    Map<String, Object> responseMap = null;

    Response response =
        cassandraOperation.getRecordsByProperty(
            geoLocationDbInfo.getKeySpace(),
            geoLocationDbInfo.getTableName(),
            JsonKey.ID,
            locationIds);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    for (Map<String, Object> map : list) {
      responseMap = new HashMap<>();
      responseMap.put(JsonKey.ID, map.get(JsonKey.ID));
      responseMap.put(
          JsonKey.USER_COUNT,
          ((map.get(JsonKey.USER_COUNT) == null) ? 0 : map.get(JsonKey.USER_COUNT)));
      result.add(responseMap);
      dbIdList.add((String) map.get(JsonKey.ID));
    }
    // For Invalid Location Id
    for (Object str : locationIds) {
      if (!dbIdList.contains((String) str)) {
        responseMap = new HashMap<>();
        responseMap.put(JsonKey.ID, str);
        responseMap.put(JsonKey.USER_COUNT, 0);
        result.add(responseMap);
      }
    }
    response = new Response();
    response.getResult().put(JsonKey.LOCATIONS, result);
    sender().tell(response, self());
    // Update user count in background
    actorMessage.setOperation(BackgroundOperations.updateUserCountToLocationID.name());
    actorMessage.getRequest().put(JsonKey.OPERATION, "GeoLocationManagementActor");
    tellToAnother(actorMessage);
  }

  private void sendNotification(Request actorMessage) {
    ProjectLogger.log("sendnotification actor method called.");
    String topic = (String) actorMessage.getRequest().get(JsonKey.TO);
    // Topic name is same as Location id in current system.
    // if logic is change then we need to update the matching logic as well
    Geolocation geolocationDetail = geolocationDao.read(topic);
    if (geolocationDetail == null) {
      // throw exception that invalid topic ...
      throw new ProjectCommonException(
          ResponseCode.invalidTopic.getErrorCode(),
          ResponseCode.invalidTopic.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> notificationData = (Map) actorMessage.getRequest().get(JsonKey.DATA);
    String message = Notification.sendNotification(topic, notificationData, Notification.FCM_URL);
    ProjectLogger.log("FCM message from Google ==" + message);
    Response response = new Response();
    if (JsonKey.FAILURE.equalsIgnoreCase(message)) {
      response.getResult().put(JsonKey.RESPONSE, JsonKey.FAILURE);
    } else {
      response.getResult().put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      response.getResult().put(JsonKey.ID, message);
    }
    sender().tell(response, self());
  }

  /**
   * Delete geo location on basis of location id.
   *
   * @param actorMessage
   */
  private void deleteGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-updateGeoLocation called");

    // object of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    String locationId = (String) actorMessage.getRequest().get(JsonKey.LOCATION_ID);
    Geolocation geolocationDetails =
        geolocationDao.read((String) actorMessage.getRequest().get(JsonKey.LOCATION_ID));
    if (geolocationDetails == null) {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationId.getErrorCode(),
          ResponseCode.invalidLocationId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Response response = geolocationDao.delete(locationId);
    sender().tell(response, self());
    targetObject =
        TelemetryUtil.generateTargetObject(locationId, JsonKey.LOCATION, JsonKey.DELETE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
  }

  /**
   * Update geo location on basis of locationId , only location type and
   *
   * @param actorMessage
   */
  private void updateGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-updateGeoLocation called");

    // object of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
    String locationId = (String) actorMessage.getRequest().get(JsonKey.LOCATION_ID);
    String type = (String) actorMessage.getRequest().get(JsonKey.TYPE);
    String location = (String) actorMessage.getRequest().get(JsonKey.LOCATION);
    Geolocation geolocationDetails = geolocationDao.read(locationId);
    if (geolocationDetails == null) {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationId.getErrorCode(),
          ResponseCode.invalidLocationId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> dbMap = new HashMap<>();
    if (!StringUtils.isBlank(type)) {
      dbMap.put(JsonKey.TYPE, type);
    }
    if (!StringUtils.isBlank(location)) {
      dbMap.put(JsonKey.LOCATION, location);
    }
    dbMap.put(JsonKey.UPDATED_BY, requestedBy);
    dbMap.put(JsonKey.UPDATED_DATE, format.format(new Date()));
    dbMap.put(JsonKey.ID, locationId);
    Response response = geolocationDao.update(dbMap);
    sender().tell(response, self());
    targetObject =
        TelemetryUtil.generateTargetObject(locationId, JsonKey.LOCATION, JsonKey.UPDATE, null);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject);
  }

  /**
   * Get geo location on basis of type and id . type should be organisation or location .
   *
   * @param actorMessage
   */
  private void getGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-getGeoLocation called");
    String id = (String) actorMessage.getRequest().get(JsonKey.ID);
    String type = (String) actorMessage.getRequest().get(JsonKey.TYPE);
    Response finalResponse = new Response();
    if (StringUtils.isBlank(id) || StringUtils.isBlank(type)) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (type.equalsIgnoreCase(JsonKey.ORGANISATION)) {
      Response response1 =
          cassandraOperation.getRecordsByProperty(
              geoLocationDbInfo.getKeySpace(),
              geoLocationDbInfo.getTableName(),
              JsonKey.ROOT_ORG_ID,
              id);
      List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);
      finalResponse.put(JsonKey.RESPONSE, list);
      sender().tell(finalResponse, self());
      return;

    } else if (type.equalsIgnoreCase(JsonKey.LOCATION)) {

      Response response1 =
          cassandraOperation.getRecordById(
              geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), id);
      List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);
      finalResponse.put(JsonKey.RESPONSE, list);
      sender().tell(finalResponse, self());
      return;
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidTypeValue.getErrorCode(),
          ResponseCode.invalidTypeValue.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  /**
   * Create geo location , and id and topic value will be same .
   *
   * @param actorMessage
   */
  private void createGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-createGeoLocation called");
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    List<Map<String, Object>> dataList =
        (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.DATA);
    Response finalResponse = new Response();
    List<Map<String, Object>> responseList = new ArrayList<>();
    String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
    String rootOrgId = (String) actorMessage.getRequest().get(JsonKey.ROOT_ORG_ID);
    Response result =
        cassandraOperation.getRecordById(
            orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), rootOrgId);
    List<Map<String, Object>> orglist = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (orglist.isEmpty()) {
      throw new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    for (Map<String, Object> dataMap : dataList) {
      String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
      Map<String, Object> geolocationMap =
          createGeolocationMap(dataMap, rootOrgId, requestedBy, id);
      geolocationDao.insert(geolocationMap);
      Map<String, Object> responseMap =
          createSuccessResponseMap(id, (String) dataMap.get(JsonKey.LOCATION));
      responseList.add(responseMap);
      targetObject = TelemetryUtil.generateTargetObject(id, JsonKey.LOCATION, JsonKey.CREATE, null);
      TelemetryUtil.generateCorrelatedObject(id, JsonKey.LOCATION, null, correlatedObject);
      TelemetryUtil.generateCorrelatedObject(rootOrgId, JsonKey.ROOT_ORG, null, correlatedObject);
      TelemetryUtil.telemetryProcessingCall(
          actorMessage.getRequest(), targetObject, correlatedObject);
    }
    finalResponse.getResult().put(JsonKey.RESPONSE, responseList);
    sender().tell(finalResponse, self());
  }

  private Map<String, Object> createGeolocationMap(
      Map<String, Object> dataMap, String rootOrgId, String requestedBy, String id) {
    String location = (String) dataMap.get(JsonKey.LOCATION);
    String type = (String) dataMap.get(JsonKey.TYPE);
    Map<String, Object> geolocationMap = new HashMap<>();
    geolocationMap.put(JsonKey.CREATED_DATE, format.format(new Date()));
    geolocationMap.put(JsonKey.CREATED_BY, requestedBy);
    geolocationMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
    geolocationMap.put(JsonKey.LOCATION, location);
    geolocationMap.put(JsonKey.TYPE, type);
    geolocationMap.put(JsonKey.ID, id);
    geolocationMap.put(JsonKey.TOPIC, id);
    return geolocationMap;
  }

  private Map<String, Object> createSuccessResponseMap(String id, String location) {
    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(JsonKey.ID, id);
    responseMap.put(JsonKey.LOCATION, location);
    responseMap.put(JsonKey.STATUS, JsonKey.SUCCESS);
    return responseMap;
  }
}
