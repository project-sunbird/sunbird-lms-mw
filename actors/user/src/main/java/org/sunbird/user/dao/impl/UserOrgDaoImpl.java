package org.sunbird.user.dao.impl;

import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.org.UserOrg;
import org.sunbird.user.dao.UserOrgDao;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserOrgDaoImpl implements UserOrgDao {

  private final Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static UserOrgDao userOrgDao = null;
  private ObjectMapper mapper = new ObjectMapper();
  private UserOrgDaoImpl() {}

  public static UserOrgDao getInstance() {
    if (userOrgDao == null) {
      userOrgDao = new UserOrgDaoImpl();
    }
    return userOrgDao;
  }

  @Override
  public Response updateUserOrg(UserOrg userOrg) {
    return cassandraOperation.updateRecord(
        userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), mapper.convertValue(userOrg, Map.class));
  }
}
