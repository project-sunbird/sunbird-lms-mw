package org.sunbird.learner.actors.bulkupload.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraQueryUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadProcessTasksDao;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTasks;

/** @author arvind. */
public class BulkUploadProcessTasksDaoImpl implements BulkUploadProcessTasksDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String TABLE_NAME = "bulk_upload_process_tasks";

  @Override
  public Response create(BulkUploadProcessTasks bulkUploadProcessTasks) {
    Map<String, Object> map = mapper.convertValue(bulkUploadProcessTasks, Map.class);
    Response response = cassandraOperation.insertRecord(KEYSPACE_NAME, TABLE_NAME, map);
    response.put(JsonKey.ID, map.get(JsonKey.ID));
    return response;
  }

  @Override
  public Response update(BulkUploadProcessTasks bulkUploadProcessTasks) {
    Map<String, Object> map = mapper.convertValue(bulkUploadProcessTasks, Map.class);
    return cassandraOperation.updateRecord(KEYSPACE_NAME, TABLE_NAME, map);
  }

  @Override
  public BulkUploadProcessTasks read(BulkUploadProcessTasks bulkUploadProcessTasks)
      throws IllegalAccessException {
    Response response =
        cassandraOperation.getRecordById(
            KEYSPACE_NAME, TABLE_NAME, CassandraQueryUtil.getPrimaryKey(bulkUploadProcessTasks));
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    try {
      String jsonString = mapper.writeValueAsString((Map<String, Object>) list.get(0));
      return mapper.readValue(jsonString, BulkUploadProcessTasks.class);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public List<BulkUploadProcessTasks> readByPrimaryKeys(Object id) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, TABLE_NAME, id);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    TypeReference<List<BulkUploadProcessTasks>> mapType =
        new TypeReference<List<BulkUploadProcessTasks>>() {};
    List<BulkUploadProcessTasks> taskList = mapper.convertValue(list, mapType);
    return taskList;
  }

  @Override
  public Response insertBatchRecord(List<BulkUploadProcessTasks> records) {
    TypeReference<List<Map<String, Object>>> tRef =
        new TypeReference<List<Map<String, Object>>>() {};
    List<Map<String, Object>> list = mapper.convertValue(records, tRef);
    Response response = cassandraOperation.batchInsert(KEYSPACE_NAME, TABLE_NAME, list);
    return response;
  }

  @Override
  public Response updateBatchRecord(List<BulkUploadProcessTasks> records)
      throws IllegalAccessException {
    List<Map<String, Map<String, Object>>> list = new ArrayList<>();
    for (BulkUploadProcessTasks bulkUploadProcessTasks : records) {
      list.add(CassandraQueryUtil.batchUpdateQuery(bulkUploadProcessTasks));
    }
    Response response = cassandraOperation.batchUpdate(KEYSPACE_NAME, TABLE_NAME, list);
    return response;
  }
}
