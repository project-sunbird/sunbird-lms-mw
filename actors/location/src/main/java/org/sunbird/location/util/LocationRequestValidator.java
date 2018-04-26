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

  /**
   * This method will validate location code
   *
   * @param code
   * @return boolean
   */
  public static boolean isValidLocationCode(String code) {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(GeoLocationJsonKey.PROPERTY_NAME, GeoLocationJsonKey.CODE);
    reqMap.put(GeoLocationJsonKey.PROPERTY_VALUE, code);
    Response response = locationDao.getRecordByProperty(reqMap);
    List<Map<String, Object>> locationMapList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    return (!locationMapList.isEmpty());
  }

  /**
   * This method will validate location type
   *
   * @param type
   * @return
   */
  public static boolean isValidLocationType(String type) {
    if (null != type && !locationTypeList.contains(type.toLowerCase())) {
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

  /**
   * This method will validate parentId , parentCode and locationType from a given request
   *
   * @param location
   * @param opType
   * @return boolean
   */
  public static boolean isValidParentIdAndCode(Map<String, Object> location, String opType) {
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
        if (StringUtils.isNotEmpty((String) location.get(GeoLocationJsonKey.CODE))) {
          isValidLocationCode(location, opType);
        }
        if (StringUtils.isNotEmpty(GeoLocationJsonKey.PARENT_CODE)
            || StringUtils.isNotEmpty(GeoLocationJsonKey.PARENT_ID)) {
          throw new ProjectCommonException(
              ResponseCode.parentNotAllowed.getErrorCode(),
              ProjectUtil.formatMessage(
                  ResponseCode.parentNotAllowed.getErrorMessage(),
                  (GeoLocationJsonKey.PARENT_ID + " or " + GeoLocationJsonKey.PARENT_CODE)),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // if type is top level then parentCode and parentId is null
        location.put(GeoLocationJsonKey.PARENT_CODE, null);
        location.put(GeoLocationJsonKey.PARENT_ID, null);
      }
    }
    String parentCode = (String) location.get(GeoLocationJsonKey.PARENT_CODE);
    String parentId = (String) location.get(GeoLocationJsonKey.PARENT_ID);
    if (StringUtils.isNotEmpty(parentCode)) {
      Map<String, Object> map = getLocation(parentCode);
      parentId = (String) map.get(JsonKey.ID);
      location.put(GeoLocationJsonKey.PARENT_ID, map.get(JsonKey.ID));
    }
    if (StringUtils.isNotEmpty(parentId)) {
      String operation = GeoLocationJsonKey.PARENT_ID;
      if (StringUtils.isNotEmpty(parentCode)) {
        operation = GeoLocationJsonKey.PARENT_CODE;
      }
      Map<String, Object> parentLocation = getLocationById(parentId, operation);
      validateParentLocationType(parentLocation, location, opType);
    }
    return true;
  }

  /**
   * This method will validate the parent location type means parent should be only one level up
   * from child
   *
   * @param parentLocation
   * @param location
   * @param opType
   * @return
   */
  private static boolean validateParentLocationType(
      Map<String, Object> parentLocation, Map<String, Object> location, String opType) {
    String parentType = (String) parentLocation.get(GeoLocationJsonKey.LOCATION_TYPE);
    String currentLocType = (String) location.get(GeoLocationJsonKey.LOCATION_TYPE);
    if (StringUtils.isNotEmpty(currentLocType)) {
      Map<String, Object> locn = null;
      if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
        locn = getLocation((String) location.get(GeoLocationJsonKey.PARENT_CODE));
      } else if (opType.equalsIgnoreCase(JsonKey.UPDATE)) {
        locn = getLocationById((String) location.get(JsonKey.ID), JsonKey.LOCATION_ID);
      }

      currentLocType = (String) locn.get(GeoLocationJsonKey.LOCATION_TYPE);
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), GeoLocationJsonKey.PARENT_ID),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
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

  /**
   * This method will return location details based on id
   *
   * @param id
   * @return Map<String, Object> location details
   */
  private static Map<String, Object> getLocationById(String id, String parameter) {
    Map<String, Object> location =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.location.getTypeName(),
            id);
    if (MapUtils.isEmpty(location)) {
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(ResponseCode.invalidParameter.getErrorMessage(), parameter),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return location;
  }

  /**
   * This method will validate location code and if code is valid will return location .
   *
   * @param location Map<String,Object> complete location details
   * @param opType (for which operation we are validating i.e CREATE or UPDATE)
   * @return location details Map<String, Object>
   */
  private static Map<String, Object> getLocation(String parentCode) {
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
      return locationMapList.get(0);
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), GeoLocationJsonKey.PARENT_CODE),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  /**
   * This method will check whether passed locationId is the parent of any location
   *
   * @param locationId
   * @return boolean
   */
  public static boolean isLocationHasChild(String locationId) {
    Map<String, Object> location = getLocationById(locationId, JsonKey.LOCATION_ID);
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

  public static boolean isValidLocationCode(Map<String, Object> location, String opType) {
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
            ResponseCode.alreadyExists.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.alreadyExists.getErrorMessage(),
                GeoLocationJsonKey.CODE,
                location.get(GeoLocationJsonKey.CODE)),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      } else if (opType.equalsIgnoreCase(JsonKey.UPDATE)) {
        Map<String, Object> locn = locationMapList.get(0);
        if (!(((String) locn.get(JsonKey.ID))
            .equalsIgnoreCase((String) location.get(JsonKey.ID)))) {
          throw new ProjectCommonException(
              ResponseCode.alreadyExists.getErrorCode(),
              ProjectUtil.formatMessage(
                  ResponseCode.alreadyExists.getErrorMessage(),
                  GeoLocationJsonKey.CODE,
                  location.get(GeoLocationJsonKey.CODE)),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    return true;
  }
}
