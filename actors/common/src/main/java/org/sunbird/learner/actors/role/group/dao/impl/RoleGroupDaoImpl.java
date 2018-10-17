package org.sunbird.learner.actors.role.group.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.group.dao.RoleGroupDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.role.group.RoleGroup;

public class RoleGroupDaoImpl implements RoleGroupDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static RoleGroupDao roleGroupDao;
  private static final String KEYSPACE_NAME = Util.dbInfoMap.get(JsonKey.ROLE_GROUP).getKeySpace();
  private static final String TABLE_NAME = Util.dbInfoMap.get(JsonKey.ROLE_GROUP).getTableName();

  public static RoleGroupDao getInstance() {
    if (roleGroupDao == null) {
      roleGroupDao = new RoleGroupDaoImpl();
    }
    return roleGroupDao;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<RoleGroup> getRoleGroups() {

    Response roleGroupResults = cassandraOperation.getAllRecords(KEYSPACE_NAME, TABLE_NAME);
    List<Map<String, Object>> roleGroupMapList =
        (List<Map<String, Object>>) roleGroupResults.get(JsonKey.RESPONSE);
    List<RoleGroup> roleGroupList = new ArrayList<>();
    for (Map<String, Object> roleGroup : roleGroupMapList) {
      roleGroupList.add(mapper.convertValue(roleGroup, RoleGroup.class));
    }
    return roleGroupList;
  }
}
