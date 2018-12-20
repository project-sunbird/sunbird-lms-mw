package org.sunbird.ratelimit.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

public class RateLimitDaoImpl implements RateLimitDao {

  private static final String TABLE_NAME = JsonKey.RATE_LIMIT;
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static volatile RateLimitDao rateLimitDao;

  public static RateLimitDao getInstance() {
    if (rateLimitDao == null) {
      synchronized (RateLimitDaoImpl.class) {
        if (rateLimitDao == null) {
          rateLimitDao = new RateLimitDaoImpl();
        }
      }
    }
    return rateLimitDao;
  }

  @Override
  public void insertRateLimits(List<Map<String, Object>> values) {
    List<Integer> ttls = new ArrayList<>();
    values.forEach(
        value -> {
          int ttl = (int) value.get(JsonKey.TTL);
          ttls.add(ttl);
          value.remove(JsonKey.TTL);
        });
    cassandraOperation.batchInsertWithTTL(Util.KEY_SPACE_NAME, TABLE_NAME, values, ttls);
  }

  @Override
  public List<Map<String, Object>> getRateLimits(String key) {
    Map<String, Object> primaryKeys = new HashMap<>();
    primaryKeys.put(JsonKey.KEY, key);
    Map<String, String> ttlPropertiesWithAlias = new HashMap<>();
    ttlPropertiesWithAlias.put(JsonKey.COUNT, JsonKey.TTL);
    Map<String, String> propertiesWithAlias = new HashMap<>();
    Arrays.asList(JsonKey.KEY, JsonKey.RATE_LIMIT_UNIT, JsonKey.COUNT)
        .stream()
        .forEach(column -> propertiesWithAlias.put(column, null));
    Response response =
        cassandraOperation.getRecordsByIdsWithSpecifiedColumnsAndTTL(
            Util.KEY_SPACE_NAME,
            TABLE_NAME,
            primaryKeys,
            propertiesWithAlias,
            ttlPropertiesWithAlias);
    return (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
  }
}
