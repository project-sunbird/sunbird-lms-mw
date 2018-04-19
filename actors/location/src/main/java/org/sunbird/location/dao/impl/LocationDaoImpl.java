package org.sunbird.location.dao.impl;

import java.util.HashMap;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.model.Location;
import org.sunbird.location.util.LocationUtil;

public class LocationDaoImpl implements LocationDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo locDbInfo = Util.dbInfoMap.get(JsonKey.LOCATION);

  @Override
  public Response create(Location location) {
    Map<String, Object> map = LocationUtil.convertPojoToMap(location);
    Response response =
        cassandraOperation.insertRecord(locDbInfo.getKeySpace(), locDbInfo.getTableName(), map);
    // need to send ID and name along with success msg
    response.put(JsonKey.ID, map.get(JsonKey.ID));
    response.put(JsonKey.NAME, map.get(JsonKey.NAME));
    return response;
  }

  @Override
  public Response update(Location location) {
    Map<String, Object> map = LocationUtil.convertPojoToMap(location);
    return cassandraOperation.updateRecord(locDbInfo.getKeySpace(), locDbInfo.getTableName(), map);
  }

  @Override
  public Response delete(String locationId) {
    return cassandraOperation.deleteRecord(
        locDbInfo.getKeySpace(), locDbInfo.getTableName(), locationId);
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
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    return null;
  }

  @Override
  public Response readAll() {
    return null;
  }
}
