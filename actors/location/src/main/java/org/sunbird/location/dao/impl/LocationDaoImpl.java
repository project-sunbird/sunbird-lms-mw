package org.sunbird.location.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.model.Location;

/** @author Amit Kumar */
public class LocationDaoImpl implements LocationDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String LOCATION_TABLE_NAME = "location";

  @Override
  public Response create(Location location) {
    Map<String, Object> map = mapper.convertValue(location, Map.class);
    Response response = cassandraOperation.insertRecord(KEYSPACE_NAME, LOCATION_TABLE_NAME, map);
    // need to send ID along with success msg
    response.put(JsonKey.ID, map.get(JsonKey.ID));
    return response;
  }

  @Override
  public Response update(Location location) {
    Map<String, Object> map = mapper.convertValue(location, Map.class);
    return cassandraOperation.updateRecord(KEYSPACE_NAME, LOCATION_TABLE_NAME, map);
  }

  @Override
  public Response delete(String locationId) {
    return cassandraOperation.deleteRecord(KEYSPACE_NAME, LOCATION_TABLE_NAME, locationId);
  }

  @Override
  public Response search(Map<String, Object> searchQueryMap) {
    SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
    String[] types = {ProjectUtil.EsType.location.getTypeName()};
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(), types);
    Response response = new Response();
    if (result != null) {
      response.put(JsonKey.RESPONSE, result.get(JsonKey.CONTENT));
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result.get(JsonKey.CONTENT));
    }
    return response;
  }

  @Override
  public Response read(String locationId) {
    return cassandraOperation.getRecordById(KEYSPACE_NAME, LOCATION_TABLE_NAME, locationId);
  }

  @Override
  public Response getRecordByProperty(Map<String, Object> queryMap) {
    return cassandraOperation.getRecordsByProperty(
        KEYSPACE_NAME,
        LOCATION_TABLE_NAME,
        (String) queryMap.get(GeoLocationJsonKey.PROPERTY_NAME),
        queryMap.get(GeoLocationJsonKey.PROPERTY_VALUE));
  }
}
