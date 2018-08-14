package org.sunbird.learner.actors.skill.dao;

import java.util.List;
import java.util.Map;

public interface UserSkillDao {

  /**
   * This method will add skills for user ProjectCommonException.
   *
   * @param userSkill map containing information about user skill
   */
  void add(Map<String, Object> userSkill);

  /**
   * This method will delete skill for user ProjectCommonException.
   *
   * @param identifierList list of id which needs to be deleted
   * @return boolean
   */
  boolean delete(List<String> identifierList);
}
