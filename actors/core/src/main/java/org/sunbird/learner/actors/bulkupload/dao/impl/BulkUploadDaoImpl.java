package org.sunbird.learner.actors.bulkupload.dao.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadDao;
import org.sunbird.learner.actors.bulkupload.model.BulkUpload;

/** Created by arvind on 24/4/18. */
public class BulkUploadDaoImpl implements BulkUploadDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String TABLE_NAME = "bulk_upload_process";

  @Override
  public Response create(BulkUpload bulkUpload) {
    Map<String, Object> map = mapper.convertValue(bulkUpload, Map.class);
    Response response = cassandraOperation.insertRecord(KEYSPACE_NAME, TABLE_NAME, map);
    // need to send ID along with success msg
    response.put(JsonKey.ID, map.get(JsonKey.ID));
    return response;
  }

  @Override
  public Response update(BulkUpload bulkUpload) {
    Map<String, Object> map = mapper.convertValue(bulkUpload, Map.class);
    return cassandraOperation.updateRecord(KEYSPACE_NAME, TABLE_NAME, map);
  }

  @Override
  public BulkUpload read(String id) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, TABLE_NAME, id);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    try {
      String jsonString = mapper.writeValueAsString((Map<String, Object>) list.get(0));
      return mapper.readValue(jsonString, BulkUpload.class);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public Response getRecordByProperty(Map<String, Object> queryMap) {
    return cassandraOperation.getRecordsByProperty(
        KEYSPACE_NAME,
        TABLE_NAME,
        (String) queryMap.get(GeoLocationJsonKey.PROPERTY_NAME),
        queryMap.get(GeoLocationJsonKey.PROPERTY_VALUE));
  }
}
