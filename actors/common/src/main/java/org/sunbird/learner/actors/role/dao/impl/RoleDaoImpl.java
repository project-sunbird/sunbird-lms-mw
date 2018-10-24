package org.sunbird.learner.actors.role.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.dao.RoleDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.role.Role;

public class RoleDaoImpl implements RoleDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static RoleDao roleDao;
  private final Util.DbInfo roleDbInfo  = Util.dbInfoMap.get(JsonKey.ROLE);

  public static RoleDao getInstance() {
    if (roleDao == null) {
      roleDao = new RoleDaoImpl();
    }
    return roleDao;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Role> getRoles() {
    Response roleResults = cassandraOperation.getAllRecords(roleDbInfo.getKeySpace(), roleDbInfo.getTableName());
    TypeReference<List<Role>> roleMapType = new TypeReference<List<Role>>() {};
    List<Map<String, Object>> roleMapList =
        (List<Map<String, Object>>) roleResults.get(JsonKey.RESPONSE);
    List<Role> roleList = mapper.convertValue(roleMapList, roleMapType);
    return roleList;
  }

}
