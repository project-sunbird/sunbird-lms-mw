package org.sunbird.learner.actors.coursebatch.dao.impl;

import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchManagementDao;
import org.sunbird.learner.util.Util;

public class CourseBatchManagementDaoImpl implements CourseBatchManagementDao {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo courseBatchDb = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  static CourseBatchManagementDao courseBatchManagementDao;
  Util.DbInfo coursePublishdb = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
  Util.DbInfo courseEnrollmentdb = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);

  public static CourseBatchManagementDao getInstance() {
    if (courseBatchManagementDao == null) {
      courseBatchManagementDao = new CourseBatchManagementDaoImpl();
    }
    return courseBatchManagementDao;
  }

  @Override
  public Response create(Map<String, Object> map) {
    return cassandraOperation.insertRecord(
        courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), map);
  }

  @Override
  public Response update(Map<String, Object> map) {
    return cassandraOperation.updateRecord(
        courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), map);
  }

  @Override
  public List<Map<String, Object>> readById(String id) {
    Response courseBatchResult =
        cassandraOperation.getRecordById(
            courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), id);
    List<Map<String, Object>> courseList =
        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return courseList;
  }

  @Override
  public List<Map<String, Object>> readAll() {
    return null;
  }

  @Override
  public Response delete(String id) {
    return cassandraOperation.deleteRecord(
        courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), id);
  }

  @Override
  public List<Map<String, Object>> readPublishedCourse(String id) {
    Response courseBatchResult =
        cassandraOperation.getRecordById(
            coursePublishdb.getKeySpace(), coursePublishdb.getTableName(), id);
    List<Map<String, Object>> publishedCourse =
        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((publishedCourse.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return publishedCourse;
  }

  @Override
  public Response createCourseEnrolment(Map<String, Object> map) {
    return cassandraOperation.insertRecord(
        courseEnrollmentdb.getKeySpace(), courseEnrollmentdb.getTableName(), map);
  }
}
