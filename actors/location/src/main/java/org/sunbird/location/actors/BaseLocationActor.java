package org.sunbird.location.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
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
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.dao.impl.LocationDaoImpl;

/** @author Amit Kumar */
public class BaseLocationActor extends BaseActor {
  protected static List<String> locationTypeList = null;
  protected static Map<String, Integer> locationTypeOrderMap = null;

  static {
    locationTypeList =
        Arrays.asList(ProjectUtil.getConfigValue(GeoLocationJsonKey.LOCATION_TYPE).split(","));
    for (int i = 0; i < locationTypeList.size(); i++) {
      locationTypeOrderMap.put(locationTypeList.get(i), i);
    }
  }

  private LocationDao locationDao = new LocationDaoImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    onReceiveUnsupportedOperation("BaseLocationActor");
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
    String type = (String) location.get(JsonKey.TYPE);
    // if type is of top level then no need to validate parentCode and parentId
    if (!locationTypeList.get(0).equalsIgnoreCase(type)
        && (StringUtils.isEmpty((String) location.get(GeoLocationJsonKey.PARENT_CODE))
            && StringUtils.isEmpty((String) location.get(GeoLocationJsonKey.PARENT_ID)))) {
      throw new ProjectCommonException(
          ResponseCode.parentCodeAndIdValidationError.getErrorCode(),
          ResponseCode.parentCodeAndIdValidationError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      // if type is top level then parentCode and parentId is null
      location.put(GeoLocationJsonKey.PARENT_CODE, null);
      location.put(GeoLocationJsonKey.PARENT_ID, null);
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

    String parentType = (String) parentLocation.get(JsonKey.TYPE);
    String currentLocType = (String) location.get(JsonKey.TYPE);
    if ((locationTypeOrderMap.get(currentLocType) - locationTypeOrderMap.get(parentType)) != 1) {
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

  public boolean validateDeleteRequest(String locationId) {
    Map<String, Object> location = getLocationById(locationId);
    List<Integer> list = new ArrayList<>(locationTypeOrderMap.values());
    list.sort(Comparator.reverseOrder());
    int order = locationTypeOrderMap.get(location.get(JsonKey.TYPE));
    // location type with last order can be deleted without validation
    if (order != list.get(0)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(GeoLocationJsonKey.PARENT_ID, location.get(GeoLocationJsonKey.PARENT_ID));
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

    return false;
  }

  public void validateParentIdWithType(Map<String, Object> location) {
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

    String subLocnType = (String) location.get(JsonKey.TYPE);
    String parentLocnType = (String) parentLocMap.get(JsonKey.TYPE);

    if ((locationTypeOrderMap.get(subLocnType) - locationTypeOrderMap.get(parentLocnType)) != 1) {
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
}
