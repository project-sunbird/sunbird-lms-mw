package org.sunbird.learner.actors.coursebatch.dao.impl;

import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.UserCourseDao;
import org.sunbird.learner.util.Util;

/** Created by rajatgupta on 25/09/18. */
public class UserCourseDaoImpl implements UserCourseDao {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static UserCourseDaoImpl userCourseDao;

  private Util.DbInfo userCourseDb = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);

  @Override
  public Response createCourseEnrolment(Map<String, Object> map) {
    return cassandraOperation.insertRecord(
        userCourseDb.getKeySpace(), userCourseDb.getTableName(), map);
  }
}
