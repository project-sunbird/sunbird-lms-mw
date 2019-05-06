package org.sunbird.common.cacheloader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.redisson.api.RMap;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.redis.RedisConnectionManager;

public class CacheLoaderService implements Runnable {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static final String KEY_SPACE_NAME = "sunbird";

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
      ProjectLogger.log(
          "CacheLoaderService:cacheLoader: Exception in retrieving page section " + e.getMessage(),
          e);
    }
    return map;
  }

  void loadPageSectionInCache(
      List<Map<String, Object>> responseList, Map<String, Map<String, Object>> map) {

    for (Map<String, Object> resultMap : responseList) {

      map.put((String) resultMap.get(JsonKey.ID), resultMap);
    }
  }

  void loadPagesInCache(
      List<Map<String, Object>> responseList, Map<String, Map<String, Object>> map) {
    for (Map<String, Object> resultMap : responseList) {
      String orgId =
          (((String) resultMap.get(JsonKey.ORGANISATION_ID)) == null
              ? "NA"
              : (String) resultMap.get(JsonKey.ORGANISATION_ID));
      map.put(orgId + ":" + ((String) resultMap.get(JsonKey.PAGE_NAME)), resultMap);
    }
  }

  @Override
  public void run() {
    updateAllCache();
  }

  private void updateAllCache() {
    updateCache(cacheLoader("page_section"), ActorOperations.GET_SECTION.getValue());
    updateCache(cacheLoader("page_management"), ActorOperations.GET_PAGE_DATA.getValue());
    ProjectLogger.log("CacheLoaderService:updateAllCache completed", LoggerEnum.INFO.name());
  }

  private static void updateCache(Map<String, Map<String, Object>> cache, String mapName) {
    RMap<Object, Object> map = RedisConnectionManager.getClient().getMap(mapName);
    Set<String> keys = cache.keySet();
    for (String key : keys) {
      String value = ProjectUtil.getJsonString(cache.get(key));
      map.put(key, value);
    }
  }
}
