package org.sunbird.learner.actors.role.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.dao.RoleDao;
import org.sunbird.models.role.Role;

public class RoleDaoImpl implements RoleDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static RoleDao roleDao;
  private static final String KEYSPACE_NAME = "sunbird";
  private static final String TABLE_NAME = "role";

  public static RoleDao getInstance() {
    if (roleDao == null) {
      roleDao = new RoleDaoImpl();
    }
    return roleDao;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Role> getRoles() {

    Response roleResults = cassandraOperation.getAllRecords(KEYSPACE_NAME, TABLE_NAME);
    List<Map<String, Object>> roleMapList =
        (List<Map<String, Object>>) roleResults.get(JsonKey.RESPONSE);
    List<Role> roleList = new ArrayList<>();
    for (Map<String, Object> role : roleMapList) {
      roleList.add(mapper.convertValue(role, Role.class));
    }
    return roleList;
  }
}
