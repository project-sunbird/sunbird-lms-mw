package org.sunbird.learner.actors.bulkupload.dao;

import java.util.List;
import org.sunbird.common.models.response.Response;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTasks;

/** Created by arvind on 3/5/18. */
public interface BulkUploadProcessTasksDao {

  Response create(BulkUploadProcessTasks bulkUploadProcessTasks);

  Response update(BulkUploadProcessTasks bulkUploadProcessTasks);

  BulkUploadProcessTasks read(BulkUploadProcessTasks id) throws IllegalAccessException;

  List<BulkUploadProcessTasks> readByPrimaryKeys(Object id);

  Response insertBatchRecord(List<BulkUploadProcessTasks> records);

  Response updateBatchRecord(List<BulkUploadProcessTasks> records) throws IllegalAccessException;
}
