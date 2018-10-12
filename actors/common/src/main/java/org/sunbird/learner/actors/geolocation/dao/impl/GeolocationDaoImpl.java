package org.sunbird.learner.actors.geolocation.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.geolocation.dao.GeolocationDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.geolocation.Geolocation;

public class GeolocationDaoImpl implements GeolocationDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  static GeolocationDao geolocationDao;
  private static final String KEYSPACE_NAME =
      Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB).getKeySpace();
  private static final String TABLE_NAME =
      Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB).getTableName();

  public static GeolocationDao getInstance() {
    if (geolocationDao == null) {
      geolocationDao = new GeolocationDaoImpl();
    }
    return geolocationDao;
  }

  @Override
  public Geolocation read(String id) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, TABLE_NAME, id);
    List<Map<String, Object>> geolocationList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(geolocationList)) {
      return null;
    }
    try {
      return mapper.convertValue((Map<String, Object>) geolocationList.get(0), Geolocation.class);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public Response update(Map<String, Object> updateAttributes) {
    return cassandraOperation.updateRecord(KEYSPACE_NAME, TABLE_NAME, updateAttributes);
  }

  @Override
  public Response insert(Map<String, Object> geoLocationDetails) {
    return cassandraOperation.insertRecord(KEYSPACE_NAME, TABLE_NAME, geoLocationDetails);
  }

  @Override
  public Response delete(String id) {
    return cassandraOperation.deleteRecord(KEYSPACE_NAME, TABLE_NAME, id);
  }
}
