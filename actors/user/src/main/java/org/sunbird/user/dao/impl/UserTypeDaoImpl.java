package org.sunbird.user.dao.impl;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.UserTypeDao;

public class UserTypeDaoImpl implements UserTypeDao {

  private static final String TABLE_NAME = "user_type";
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static UserTypeDao userTypeDao = null;

  public static UserTypeDao getInstance() {
    if (userTypeDao == null) {
      userTypeDao = new UserTypeDaoImpl();
    }
    return userTypeDao;
  }

  @Override
  public Response getUserTypes() {
    return cassandraOperation.getAllRecords(Util.KEY_SPACE_NAME, TABLE_NAME);
  }
}
