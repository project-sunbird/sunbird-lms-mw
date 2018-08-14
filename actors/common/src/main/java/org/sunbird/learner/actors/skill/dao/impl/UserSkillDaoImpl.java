package org.sunbird.learner.actors.skill.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.skill.dao.UserSkillDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.skill.Skill;

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
  public void addUserSkill(Map<String, Object> userSkill) {
    cassandraOperation.insertRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), userSkill);
  }

  @Override
  public boolean deleteUserSkill(List<String> idList) {
    return cassandraOperation.deleteRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), idList);
  }

  @Override
  public Skill read(String id) {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.convertValue(
        cassandraOperation.getRecordById(
            userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), id),
        Skill.class);
  }

  @Override
  public void update(Skill skill) {
    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, String>> typeRef =
        new TypeReference<HashMap<String, String>>() {};
    HashMap<String, Object> map = objectMapper.convertValue(skill, typeRef);
    cassandraOperation.updateRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), map);
  }
}
