package org.sunbird.location.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryLmaxWriter;
import org.sunbird.telemetry.util.TelemetryUtil;

/** @author Amit Kumar */
public abstract class BaseLocationActor extends BaseActor {

  protected static List<String> locationTypeList = null;
  protected static Map<String, Integer> locationTypeOrderMap = null;

  static {
    locationTypeList =
        Arrays.asList(ProjectUtil.getConfigValue(GeoLocationJsonKey.LOCATION_TYPE).split(","));
    locationTypeOrderMap = new LinkedHashMap<>();
    for (int i = 0; i < locationTypeList.size(); i++) {
      locationTypeOrderMap.put(locationTypeList.get(i), i);
    }
  }

  public boolean isValidLocationCode(Map<String, Object> location, String opType) {
    Map<String, Object> filters = new HashMap<>();
    filters.put(GeoLocationJsonKey.CODE, location.get(GeoLocationJsonKey.CODE));
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.FILTERS, filters);
    List<Map<String, Object>> locationMapList =
        getESSearchResult(
            map,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName());
    if (!locationMapList.isEmpty()) {
      if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
        throw new ProjectCommonException(
            ResponseCode.locationCodeAlreadyExist.getErrorCode(),
            ResponseCode.locationCodeAlreadyExist.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      } else if (opType.equalsIgnoreCase(JsonKey.UPDATE)) {
        Map<String, Object> locn = locationMapList.get(0);
        if (!(((String) locn.get(JsonKey.ID))
            .equalsIgnoreCase((String) location.get(JsonKey.ID)))) {
          throw new ProjectCommonException(
              ResponseCode.locationCodeAlreadyExist.getErrorCode(),
              ResponseCode.locationCodeAlreadyExist.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    return true;
  }

  public boolean isValidLocationType(String type) {

    if (null != type && !locationTypeList.contains(type)) {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationType.getErrorCode(),
          ResponseCode.invalidLocationType.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return true;
  }

  public boolean isValidParentIdAndCode(Map<String, Object> location) {
    String type = (String) location.get(GeoLocationJsonKey.LOCATION_TYPE);
    if (StringUtils.isNotEmpty(type)) {
      // if type is of top level then no need to validate parentCode and parentId
      if (!locationTypeList.get(0).equalsIgnoreCase(type.toLowerCase())) {
        // for creating new location type and parent id is mandatory
        if ((StringUtils.isEmpty((String) location.get(GeoLocationJsonKey.PARENT_CODE))
            && StringUtils.isEmpty((String) location.get(GeoLocationJsonKey.PARENT_ID)))) {
          throw new ProjectCommonException(
              ResponseCode.parentCodeAndIdValidationError.getErrorCode(),
              ResponseCode.parentCodeAndIdValidationError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      } else if (locationTypeList.get(0).equalsIgnoreCase(type.toLowerCase())) {
        // if type is top level then parentCode and parentId is null
        location.put(GeoLocationJsonKey.PARENT_CODE, null);
        location.put(GeoLocationJsonKey.PARENT_ID, null);
      }
    }
    String parentCode = (String) location.get(GeoLocationJsonKey.PARENT_CODE);
    String parentId = (String) location.get(GeoLocationJsonKey.PARENT_ID);
    if (StringUtils.isNotEmpty(parentCode)) {
      parentId = getParentIdFromParentCode(parentCode);
      location.put(GeoLocationJsonKey.PARENT_ID, parentId);
    }
    if (StringUtils.isNotEmpty(parentId)) {
      Map<String, Object> parentLocation = getLocationById(parentId);
      validateTypeWithParentLocn(parentLocation, location);
    }
    return true;
  }

  private boolean validateTypeWithParentLocn(
      Map<String, Object> parentLocation, Map<String, Object> location) {
    Map<String, Object> locn = getLocationById((String) location.get(JsonKey.ID));
    String parentType = (String) parentLocation.get(GeoLocationJsonKey.LOCATION_TYPE);
    String currentLocType = (String) locn.get(GeoLocationJsonKey.LOCATION_TYPE);
    if ((locationTypeOrderMap.get(currentLocType.toLowerCase())
            - locationTypeOrderMap.get(parentType.toLowerCase()))
        != 1) {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationParentId.getErrorCode(),
          ResponseCode.invalidLocationParentId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return false;
  }

  private Map<String, Object> getLocationById(String parentId) {
    Map<String, Object> location =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName(),
            parentId);
    if (MapUtils.isEmpty(location)) {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationParentId.getErrorCode(),
          ResponseCode.invalidLocationParentId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return location;
  }

  private String getParentIdFromParentCode(String parentCode) {
    Map<String, Object> filters = new HashMap<>();
    filters.put(GeoLocationJsonKey.CODE, parentCode);
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.FILTERS, filters);
    List<Map<String, Object>> locationMapList =
        getESSearchResult(
            map,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName());
    if (CollectionUtils.isNotEmpty(locationMapList)) {
      Map<String, Object> location = locationMapList.get(0);
      return (String) location.get(JsonKey.ID);
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidLocationParentCode.getErrorCode(),
          ResponseCode.invalidLocationParentCode.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public boolean isLocationHasSubLocation(String locationId) {
    Map<String, Object> location = getLocationById(locationId);
    List<Integer> list = new ArrayList<>(locationTypeOrderMap.values());
    list.sort(Comparator.reverseOrder());
    int order =
        locationTypeOrderMap.get(
            ((String) location.get(GeoLocationJsonKey.LOCATION_TYPE)).toLowerCase());
    // location type with last order can be deleted without validation
    if (order != list.get(0)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(GeoLocationJsonKey.PARENT_ID, location.get(JsonKey.ID));
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.FILTERS, filters);
      List<Map<String, Object>> locationMapList =
          getESSearchResult(
              map,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.location.getTypeName());
      if (CollectionUtils.isNotEmpty(locationMapList)) {
        throw new ProjectCommonException(
            ResponseCode.invalidLocationDeleteRequest.getErrorCode(),
            ResponseCode.invalidLocationDeleteRequest.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return true;
  }

  public void validateParentIdWithType(Map<String, Object> location) {
    if (locationTypeList
        .get(0)
        .equalsIgnoreCase((String) location.get(GeoLocationJsonKey.LOCATION_TYPE))) {
      return;
    }
    Map<String, Object> locMap =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName(),
            (String) location.get(JsonKey.ID));

    Map<String, Object> parentLocMap =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName(),
            (String) locMap.get(GeoLocationJsonKey.PARENT_ID));

    String subLocnType = (String) location.get(GeoLocationJsonKey.LOCATION_TYPE);
    String parentLocnType = (String) parentLocMap.get(GeoLocationJsonKey.LOCATION_TYPE);

    if ((locationTypeOrderMap.get(subLocnType.toLowerCase())
            - locationTypeOrderMap.get(parentLocnType.toLowerCase()))
        != 1) {
      throw new ProjectCommonException(
          ResponseCode.locationTypeConflicts.getErrorCode(),
          ResponseCode.locationTypeConflicts.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public List<Map<String, Object>> getESSearchResult(
      Map<String, Object> searchQueryMap, String esIndex, String esType) {
    SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
    Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto, esIndex, esType);
    return (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
  }

  public void generateTelemetryForLocation(
      String targetObjId, Map<String, Object> data, String operation) {
    // object of telemetry event...
    try {
      Map<String, Object> targetObject = null;
      List<Map<String, Object>> correlatedObject = new ArrayList<>();
      targetObject =
          TelemetryUtil.generateTargetObject(targetObjId, JsonKey.LOCATION, operation, null);
      if (!MapUtils.isEmpty(data)
          && StringUtils.isNotEmpty((String) data.get(GeoLocationJsonKey.PARENT_ID))) {
        TelemetryUtil.generateCorrelatedObject(
            (String) data.get(GeoLocationJsonKey.PARENT_ID),
            JsonKey.LOCATION,
            null,
            correlatedObject);
      }
      TelemetryUtil.telemetryProcessingCall(data, targetObject, correlatedObject);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  public void generateSearchTelemetryEvent(
      SearchDTO searchDto, String[] types, Map<String, Object> result) {
    try {
      Map<String, Object> telemetryContext = TelemetryUtil.getTelemetryContext();
      Map<String, Object> params = new HashMap<>();
      params.put(JsonKey.QUERY, searchDto.getQuery());
      params.put(JsonKey.FILTERS, searchDto.getAdditionalProperties().get(JsonKey.FILTERS));
      params.put(JsonKey.SORT, searchDto.getSortBy());
      params.put(JsonKey.TOPN, generateTopNResult(result));
      params.put(JsonKey.SIZE, result.get(JsonKey.COUNT));
      params.put(JsonKey.TYPE, String.join(",", types));

      Request request = new Request();
      request.setRequest(telemetryRequestForSearch(telemetryContext, params));
      TelemetryLmaxWriter.getInstance().submitMessage(request);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  private List<Map<String, Object>> generateTopNResult(Map<String, Object> result) {
    List<Map<String, Object>> dataMapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    Integer topN =
        Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.SEARCH_TOP_N));
    int count = Math.min(topN, dataMapList.size());
    List<Map<String, Object>> list = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> m = new HashMap<>();
      m.put(JsonKey.ID, dataMapList.get(i).get(JsonKey.ID));
      list.add(m);
    }
    return list;
  }

  private static Map<String, Object> telemetryRequestForSearch(
      Map<String, Object> telemetryContext, Map<String, Object> params) {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTEXT, telemetryContext);
    map.put(JsonKey.PARAMS, params);
    map.put(JsonKey.TELEMETRY_EVENT_TYPE, "SEARCH");
    return map;
  }
}
