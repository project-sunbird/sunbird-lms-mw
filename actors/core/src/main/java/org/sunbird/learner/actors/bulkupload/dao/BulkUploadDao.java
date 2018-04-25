package org.sunbird.learner.actors.bulkupload.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.learner.actors.bulkupload.model.BulkUpload;

/** Created by arvind on 24/4/18. */
public interface BulkUploadDao {

  /**
   * @param bulkUpload Location Details
   * @return response Response
   */
  Response create(BulkUpload bulkUpload);

  /**
   * @param bulkUpload Location Details
   * @return response Response
   */
  Response update(BulkUpload bulkUpload);

  /**
   * @param id
   * @return response Response
   */
  BulkUpload read(String id);

  /**
   * @param queryMap
   * @return response Response
   */
  Response getRecordByProperty(Map<String, Object> queryMap);
}
