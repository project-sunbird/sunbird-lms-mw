package org.sunbird.location.util;

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
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.dao.impl.LocationDaoImpl;

/** @author Amit Kumar */
public class LocationRequestValidator {

  private LocationRequestValidator() {}

  private static LocationDao locationDao = new LocationDaoImpl();
  protected static List<String> locationTypeList = null;
  protected static Map<String, Integer> locationTypeOrderMap = null;

  static {
    locationTypeList =
        Arrays.asList(
            ProjectUtil.getConfigValue(GeoLocationJsonKey.SUNBIRD_VALID_LOCATION_TYPES).split(","));
    locationTypeOrderMap = new LinkedHashMap<>();
    for (int i = 0; i < locationTypeList.size(); i++) {
      locationTypeOrderMap.put(locationTypeList.get(i), i);
    }
  }

  public static boolean isValidLocationCode(Map<String, Object> location, String opType) {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(GeoLocationJsonKey.PROPERTY_NAME, GeoLocationJsonKey.CODE);
    reqMap.put(GeoLocationJsonKey.PROPERTY_VALUE, location.get(GeoLocationJsonKey.CODE));
    Response response = locationDao.getRecordByProperty(reqMap);
    List<Map<String, Object>> locationMapList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!locationMapList.isEmpty()) {
      if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
        throw new ProjectCommonException(
            ResponseCode.alreadyExist.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.alreadyExist.getErrorMessage(), GeoLocationJsonKey.CODE),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      } else if (opType.equalsIgnoreCase(JsonKey.UPDATE)) {
        Map<String, Object> locn = locationMapList.get(0);
        if (!(((String) locn.get(JsonKey.ID))
            .equalsIgnoreCase((String) location.get(JsonKey.ID)))) {
          throw new ProjectCommonException(
              ResponseCode.alreadyExist.getErrorCode(),
              ProjectUtil.formatMessage(
                  ResponseCode.alreadyExist.getErrorMessage(), GeoLocationJsonKey.CODE),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    return true;
  }

  public static boolean isValidLocationType(String type) {

    if (null != type && !locationTypeList.contains(type)) {
      throw new ProjectCommonException(
          ResponseCode.invalidValue.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidValue.getErrorMessage(),
              type,
              GeoLocationJsonKey.LOCATION_TYPE,
              locationTypeList),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return true;
  }

  public static boolean isValidParentIdAndCode(Map<String, Object> location) {
    String type = (String) location.get(GeoLocationJsonKey.LOCATION_TYPE);
    if (StringUtils.isNotEmpty(type)) {
      // if type is of top level then no need to validate parentCode and parentId
      if (!locationTypeList.get(0).equalsIgnoreCase(type.toLowerCase())) {
        // while creating new location, if locationType is not top level then type and parent id is
        // mandatory
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
      parentId = getLocationId(parentCode);
      location.put(GeoLocationJsonKey.PARENT_ID, parentId);
    }
    if (StringUtils.isNotEmpty(parentId)) {
      Map<String, Object> parentLocation = getLocationById(parentId);
      validateParentLocationType(parentLocation, location);
    }
    return true;
  }

  private static boolean validateParentLocationType(
      Map<String, Object> parentLocation, Map<String, Object> location) {
    Map<String, Object> locn = getLocationById((String) location.get(JsonKey.ID));
    String parentType = (String) parentLocation.get(GeoLocationJsonKey.LOCATION_TYPE);
    String currentLocType = (String) locn.get(GeoLocationJsonKey.LOCATION_TYPE);
    if ((locationTypeOrderMap.get(currentLocType.toLowerCase())
            - locationTypeOrderMap.get(parentType.toLowerCase()))
        != 1) {
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), GeoLocationJsonKey.PARENT_ID),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return false;
  }

  private static Map<String, Object> getLocationById(String parentId) {
    Map<String, Object> location =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName(),
            parentId);
    if (MapUtils.isEmpty(location)) {
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), JsonKey.LOCATION_ID),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return location;
  }

  private static String getLocationId(String parentCode) {
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
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), GeoLocationJsonKey.PARENT_CODE),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public static boolean isLocationHasChild(String locationId) {
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

  public static List<Map<String, Object>> getESSearchResult(
      Map<String, Object> searchQueryMap, String esIndex, String esType) {
    SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
    Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto, esIndex, esType);
    return (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
  }
}
