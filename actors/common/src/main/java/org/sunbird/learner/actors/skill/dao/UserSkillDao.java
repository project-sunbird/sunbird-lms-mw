package org.sunbird.learner.actors.skill.dao;

import java.util.List;
import java.util.Map;

public interface UserSkillDao {

  /** This method will add skills for user ProjectCommonException. */
  void add(Map<String, Object> userSkill);

  /** This method will delete skill for user ProjectCommonException. */
  boolean delete(List<String> identifierList);
}
