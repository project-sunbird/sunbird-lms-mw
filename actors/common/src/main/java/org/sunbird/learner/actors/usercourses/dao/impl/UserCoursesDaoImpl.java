package org.sunbird.learner.actors.usercourses.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.usercourses.dao.UserCoursesDao;
import org.sunbird.models.user.courses.UserCourses;

public class UserCoursesDaoImpl implements UserCoursesDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  static UserCoursesDao userCoursesDao;
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String TABLE_NAME = "user_courses";

  public static UserCoursesDao getInstance() {
    if (userCoursesDao == null) {
      userCoursesDao = new UserCoursesDaoImpl();
    }
    return userCoursesDao;
  }

  @Override
  public UserCourses read(String id) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, TABLE_NAME, id);
    List<Map<String, Object>> userCoursesList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      return mapper.convertValue((Map<String, Object>) userCoursesList.get(0), UserCourses.class);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public Response update(Map<String, Object> updateAttributes, Map<String, Object> compositeKey) {
    return cassandraOperation.updateRecord(
        KEYSPACE_NAME, TABLE_NAME, updateAttributes, compositeKey);
  }

  @Override
  public Response insert(Map<String, Object> userCoursesDetails) {
    return cassandraOperation.insertRecord(KEYSPACE_NAME, TABLE_NAME, userCoursesDetails);
  }
}
