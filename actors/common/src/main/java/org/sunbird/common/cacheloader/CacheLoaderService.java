package org.sunbird.common.cacheloader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.redisson.api.RMap;
import org.sunbird.cache.CacheFactory;
import org.sunbird.cache.interfaces.Cache;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.notification.utils.JsonUtil;
import org.sunbird.redis.RedisConnectionManager;

public class CacheLoaderService implements Runnable {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static final String KEY_SPACE_NAME = "sunbird";
  private boolean cacheEnable =
          Boolean.parseBoolean(ProjectUtil.propertiesCache.getProperty(JsonKey.SUNBIRD_CACHE_ENABLE));
  private Cache cache = CacheFactory.getInstance();
  private Util.DbInfo sectionDbInfo = Util.dbInfoMap.get(JsonKey.SECTION_MGMT_DB);
  @SuppressWarnings("unchecked")
  public Map<String, Map<String, Object>> cacheLoader(String tableName) {
    Map<String, Map<String, Object>> map = new HashMap<>();
    try {
      Response response = cassandraOperation.getAllRecords(KEY_SPACE_NAME, tableName);
      List<Map<String, Object>> responseList =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (null != responseList && !responseList.isEmpty()) {
        if (tableName.equalsIgnoreCase(JsonKey.PAGE_SECTION)) {
          loadPageSectionInCache(responseList, map);
        } else if (tableName.equalsIgnoreCase("page_management")) {
          loadPagesInCache(responseList, map);
        }
      }
    } catch (Exception e) {
      ProjectLogger.log("CacheLoaderService:cacheLoader: Exception occurred = " + e.getMessage(), e);
    }
    return map;
  }

  void loadPageSectionInCache(
      List<Map<String, Object>> responseList, Map<String, Map<String, Object>> map) {

    for (Map<String, Object> resultMap : responseList) {
      removeUnwantedData(resultMap,"");
      map.put((String) resultMap.get(JsonKey.ID), resultMap);
    }
  }

  void loadPagesInCache(
      List<Map<String, Object>> responseList, Map<String, Map<String, Object>> map) {

    for (Map<String, Object> resultMap : responseList) {
      String pageName = (String) resultMap.get(JsonKey.PAGE_NAME);
      String orgId = (String) resultMap.get(JsonKey.ORGANISATION_ID);
      if (orgId == null) {
        orgId = "NA";
      }
      map.put(orgId + ":" + pageName, resultMap);
    }
  }

  @Override
  public void run() {
    updateAllCache();
  }

  private void updateAllCache() {
    ProjectLogger.log("CacheLoaderService: updateAllCache called", LoggerEnum.INFO.name());

    updateCache(cacheLoader(JsonKey.PAGE_SECTION), ActorOperations.GET_SECTION.getValue());
    updateCache(cacheLoader(JsonKey.PAGE_MANAGEMENT), ActorOperations.GET_PAGE_DATA.getValue());
  }
  private void removeUnwantedData(Map<String, Object> map, String from) {
    map.remove(JsonKey.CREATED_DATE);
    map.remove(JsonKey.CREATED_BY);
    map.remove(JsonKey.UPDATED_DATE);
    map.remove(JsonKey.UPDATED_BY);
    if (from.equalsIgnoreCase("getPageData")) {
      map.remove(JsonKey.STATUS);
    }
  }

  public String getAllSections(){

    String res = null;
    if (cacheEnable) {
      res = cache.get(ActorOperations.GET_ALL_SECTION.getValue(), JsonKey.SECTION);
      if(StringUtils.isEmpty(res)){
        Response response = getAllSectionsFromDb();

          cache.put(
                  ActorOperations.GET_ALL_SECTION.getValue(),
                  JsonKey.SECTION,
                  JsonUtil.toJson(response));

          return  JsonUtil.toJson(response);

      }else {
        return res;
      }
    }else{
      Response response = getAllSectionsFromDb();
      return JsonUtil.toJson(response);
    }

  }

  private Response getAllSectionsFromDb(){
    Response response =
            cassandraOperation.getAllRecords(
                    sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName());
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
            (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    for (Map<String, Object> map : result) {
      removeUnwantedData(map, "");
    }
    return response;
  }

  private static void updateCache(Map<String, Map<String, Object>> cache, String mapName) {
    try {
      RMap<Object, Object> map = RedisConnectionManager.getClient().getMap(mapName);
      Set<String> keys = cache.keySet();
      for (String key : keys) {
        String value = JsonUtil.toJson(cache.get(key));
        map.put(key, value);
      }
    } catch (Exception e) {
      ProjectLogger.log("CacheLoaderService:updateCache: Error occured = " + e.getMessage(), LoggerEnum.ERROR.name());
    }
  }
}
