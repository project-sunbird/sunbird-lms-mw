package org.sunbird.learner.actors.bulkupload.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessDao;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;

/** Created by arvind on 24/4/18. */
public class BulkUploadProcessDaoImpl implements BulkUploadProcessDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String TABLE_NAME = "bulk_upload_process";

  @Override
  public Response create(BulkUploadProcess bulkUploadProcess) {
    Map<String, Object> map = mapper.convertValue(bulkUploadProcess, Map.class);
    Response response = cassandraOperation.insertRecord(KEYSPACE_NAME, TABLE_NAME, map);
    // need to send ID along with success msg
    response.put(JsonKey.ID, map.get(JsonKey.ID));
    return response;
  }

  @Override
  public Response update(BulkUploadProcess bulkUploadProcess) {
    Map<String, Object> map = mapper.convertValue(bulkUploadProcess, Map.class);
    return cassandraOperation.updateRecord(KEYSPACE_NAME, TABLE_NAME, map);
  }

  @Override
  public BulkUploadProcess read(String id) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, TABLE_NAME, id);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    try {
      String jsonString = mapper.writeValueAsString((Map<String, Object>) list.get(0));
      return mapper.readValue(jsonString, BulkUploadProcess.class);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }
}
