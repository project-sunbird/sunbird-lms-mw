package org.sunbird.learner.actors.skill.dao.impl;

import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.skill.dao.UserSkillDao;
import org.sunbird.learner.util.Util;

public class UserSkillDaoImpl implements UserSkillDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo userSkillDbInfo = Util.dbInfoMap.get(JsonKey.USER_SKILL_DB);
  static UserSkillDao userSkillDao;

  public static UserSkillDao getInstance() {
    if (userSkillDao == null) {
      userSkillDao = new UserSkillDaoImpl();
    }
    return userSkillDao;
  }

  @Override
  public void add(Map<String, Object> userSkill) {
    cassandraOperation.insertRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), userSkill);
  }

  @Override
  public boolean delete(List<String> idList) {
    return cassandraOperation.deleteRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), idList);
  }
}
