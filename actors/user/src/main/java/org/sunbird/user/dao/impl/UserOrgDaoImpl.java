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

public final class UserOrgDaoImpl implements UserOrgDao {

  private final Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  private ObjectMapper mapper = new ObjectMapper();

  private UserOrgDaoImpl() {}

  private static class LazyInitializer {
    private static UserOrgDao INSTACE = new UserOrgDaoImpl();
  }

  public static UserOrgDao getInstance() {
    return LazyInitializer.INSTACE;
  }

  @Override
  public Response updateUserOrg(UserOrg userOrg) {
    return cassandraOperation.updateRecord(
        userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(),
        mapper.convertValue(userOrg, Map.class));
  }
}
